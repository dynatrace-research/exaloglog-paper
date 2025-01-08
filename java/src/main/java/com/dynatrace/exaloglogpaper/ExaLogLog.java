//
// Copyright (c) 2024-2025 Dynatrace LLC. All rights reserved.
//
// This software and associated documentation files (the "Software")
// are being made available by Dynatrace LLC for the sole purpose of
// illustrating the implementation of certain algorithms which have
// been published by Dynatrace LLC. Permission is hereby granted,
// free of charge, to any person obtaining a copy of the Software,
// to view and use the Software for internal, non-production,
// non-commercial purposes only – the Software may not be used to
// process live data or distributed, sublicensed, modified and/or
// sold either alone or as part of or in combination with any other
// software.
//
// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
// OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
// HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
// WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
// DEALINGS IN THE SOFTWARE.
//
package com.dynatrace.exaloglogpaper;

import static com.dynatrace.exaloglogpaper.DistinctCountUtil.*;
import static com.dynatrace.exaloglogpaper.MLBiasCorrectionConstants.ML_BIAS_CORRECTION_CONSTANTS;
import static java.util.Objects.requireNonNull;

import com.dynatrace.hash4j.distinctcount.StateChangeObserver;
import com.dynatrace.hash4j.util.PackedArray;
import com.dynatrace.hash4j.util.PackedArray.PackedArrayHandler;
import java.util.Arrays;

/** ExaLogLog sketch. */
public class ExaLogLog {

  private static final int V = V_MAX; // use 32-bit tokens (V + 6 == 32)
  private static final int MIN_P = 2;
  private static final int MAX_T = V - MIN_P; // the use of 32-bit tokens requires t + p <= v

  private final byte p;
  private final byte t;
  private final byte d;

  private final byte[] state;

  private ExaLogLog(byte t, byte d, byte p, byte[] state) {
    this.t = t;
    this.d = d;
    this.p = p;
    this.state = state;
  }

  private static void checkTParameter(int t) {
    if (t < 0 || t > MAX_T) {
      throw new IllegalArgumentException("illegal T parameter");
    }
  }

  private static void checkDParameter(int d, int t) {
    if (d < 0 || d > getMaxD(t)) {
      throw new IllegalArgumentException("illegal D parameter");
    }
  }

  // visible for testing
  static void checkPrecisionParameter(int p, int minP, int maxP) {
    if (p < minP || p > maxP) {
      throw new IllegalArgumentException("illegal precision parameter");
    }
  }

  /**
   * Creates an empty ExaLogLog sketch.
   *
   * @param t the t-parameter
   * @param d the d-parameter
   * @param p the precision parameter
   * @return a new ExaLogLog sketch
   */
  public static final ExaLogLog create(int t, int d, int p) {
    checkTParameter(t);
    checkDParameter(d, t);
    checkPrecisionParameter(p, getMinP(), getMaxP(t));
    return new ExaLogLog(
        (byte) t,
        (byte) d,
        (byte) p,
        PackedArray.getHandler(getRegisterBitSize(t, d)).create(getNumRegisters(p)));
  }

  /**
   * Returns the t-parameter.
   *
   * @return the t-parameter
   */
  public int getT() {
    return t;
  }

  /**
   * Returns the d-parameter.
   *
   * @return the d-parameter
   */
  public int getD() {
    return d;
  }

  private static int getNumRegisters(int p) {
    return 1 << p;
  }

  /**
   * Returns the maximum possible p-parameter for a given t-parameter.
   *
   * @param t-parameter
   * @return maximum possible p-parameter
   */
  public static final int getMaxP(int t) {
    checkTParameter(t);
    return V - t; // the use of (6+v)-bit tokens requires t + p <= v
  }

  /**
   * Returns the minimum possible p-parameter.
   *
   * @return minimum possible p-parameter
   */
  public static final int getMinP() {
    return MIN_P;
  }

  /**
   * Returns the maximum possible t-parameter.
   *
   * @return maximum possible t-parameter
   */
  public static final int getMaxT() {
    return MAX_T;
  }

  /**
   * Returns the maximum possible d-parameter for a given t-parameter.
   *
   * @param t-parameter
   * @return maximum possible d-parameter
   */
  public static final int getMaxD(int t) {
    return 64 - 6 - t;
  }

  static int getRegisterBitSize(int t, int d) {
    return 6 + t + d;
  }

  private PackedArrayHandler getPackedArrayHandler() {
    return PackedArray.getHandler(getRegisterBitSize(t, d));
  }

  /**
   * Returns a ExaLogLog sketch whose state is kept in the given byte array.
   *
   * <p>If the state is not valid (it was not retrieved using {@link #getState()} and the
   * corresponding t- and d-parameters were different) the behavior will be undefined.
   *
   * @param t the t-parameter
   * @param d the d-parameter
   * @param state the state
   * @return the new sketch
   * @throws NullPointerException if the passed array is null
   * @throws IllegalArgumentException if the passed array has invalid length
   */
  public static ExaLogLog wrap(int t, int d, byte[] state) {
    requireNonNull(state, "null argument");
    checkDParameter(d, t);
    long regBitSize = getRegisterBitSize(t, d);
    int m = (int) ((((long) state.length) << 3) / regBitSize);
    int p = 31 - Integer.numberOfLeadingZeros(m);
    if (p < MIN_P || p > getMaxP(t) || (((regBitSize << p) + 7) >>> 3) != state.length) {
      throw getUnexpectedStateLengthException();
    }
    return new ExaLogLog((byte) t, (byte) d, (byte) p, state);
  }

  /**
   * Merges two {@link ExaLogLog} sketches into a new sketch.
   *
   * <p>The precision of the merged sketch is given by the smaller precision of both sketches.
   *
   * @param sketch1 the first sketch
   * @param sketch2 the second sketch
   * @return the merged sketch
   * @throws NullPointerException if one of both arguments is null
   */
  public static ExaLogLog merge(ExaLogLog sketch1, ExaLogLog sketch2) {
    requireNonNull(sketch1, "first sketch was null");
    requireNonNull(sketch2, "second sketch was null");
    if (sketch1.t != sketch2.t) {
      throw new IllegalArgumentException("t-parameter is not equal");
    }
    if (sketch1.p <= sketch2.p) {
      if (sketch1.d <= sketch2.d) {
        return sketch1.copy().add(sketch2);
      } else {
        return sketch1.downsize(sketch2.d, sketch1.p).add(sketch2);
      }
    } else {
      if (sketch1.d >= sketch2.d) {
        return sketch2.copy().add(sketch1);
      } else {
        return sketch2.downsize(sketch1.d, sketch2.p).add(sketch1);
      }
    }
  }

  /**
   * Adds a new element represented by a 64-bit hash value to this sketch.
   *
   * <p>In order to get good estimates, it is important that the hash value is calculated using a
   * high-quality hash algorithm.
   *
   * @param hashValue a 64-bit hash value
   * @return this sketch
   */
  public ExaLogLog add(long hashValue) {
    add(hashValue, null);
    return this;
  }

  /**
   * Computes a token from a given 64-bit hash value.
   *
   * <p>Instead of updating the sketch with the hash value using the {@link #add(long)} method, it
   * can alternatively be updated with the corresponding 32-bit token using the {@link
   * #addToken(int)} method.
   *
   * <p>{@code addToken(computeToken(hash))} is equivalent to {@code add(hash)}
   *
   * <p>Tokens can be temporarily collected using for example an {@code int[] array} and added later
   * using {@link #addToken(int)} into the sketch resulting exactly in the same final state. This
   * can be used to realize a sparse mode, where the sketch is created only when there are enough
   * tokens to justify the memory allocation. It is sufficient to store only distinct tokens.
   * Deduplication does not result in any loss of information with respect to distinct count
   * estimation.
   *
   * @param hashValue the 64-bit hash value
   * @return the 32-bit token
   */
  public static int computeToken(long hashValue) {
    return DistinctCountUtil.computeToken(hashValue, V);
  }

  /**
   * Adds a new element represented by a 32-bit token obtained from {@code computeToken(long)}.
   *
   * <p>{@code addToken(computeToken(hash))} is equivalent to {@code add(hash)}
   *
   * @param token a 32-bit hash token
   * @return this sketch
   */
  public ExaLogLog addToken(int token) {
    return add(DistinctCountUtil.reconstructHash(token, V));
  }

  /**
   * Returns an estimate of the number of distinct elements added to this sketch.
   *
   * @return estimated number of distinct elements
   */
  public double getDistinctCountEstimate() {
    return getDistinctCountEstimate(null);
  }

  /**
   * Creates a copy of this sketch.
   *
   * @return the copy
   */
  public ExaLogLog copy() {
    return new ExaLogLog(t, d, p, Arrays.copyOf(state, state.length));
  }

  private static long shiftRight(long s, long delta) {
    if (delta < 64) {
      return s >>> delta;
    } else {
      return 0;
    }
  }

  private static long computeDownsizeThresholdU(int t, int fromP) {
    return ((64L - t - fromP) << t) + 1;
  }

  private static long downsizeRegister(
      long r, int t, int fromD, int toD, int fromP, int toP, int subIdx, long downsizeThresholdU) {
    long u = r >>> fromD;
    r >>>= fromD - toD;
    if (u >= downsizeThresholdU) {
      long shift = ((fromP - toP) - (32 - Integer.numberOfLeadingZeros(subIdx))) << t;
      if (shift > 0) {
        long numBitsToShift = toD + downsizeThresholdU - u;
        if (numBitsToShift > 0) {
          long mask = 0xFFFFFFFFFFFFFFFFL << numBitsToShift;
          r = (mask & r) | shiftRight((r & ~mask), shift);
        }
        r += shift << toD;
      }
    }
    return r;
  }

  private static long mergeRegister(long r1, long r2, int d) {
    long u1 = r1 >>> d;
    long u2 = r2 >>> d;
    if (u1 > u2 && u2 > 0) {
      long x = 1L << d;
      return r1 | shiftRight(x | (r2 & (x - 1)), u1 - u2);
    } else if (u2 > u1 && u1 > 0) {
      long x = 1L << d;
      return r2 | shiftRight(x | (r1 & (x - 1)), u2 - u1);
    } else {
      return r1 | r2;
    }
  }

  /**
   * Adds another sketch.
   *
   * <p>The precision parameter of the added sketch must not be smaller than the precision parameter
   * of this sketch. Otherwise, an {@link IllegalArgumentException} will be thrown.
   *
   * @param other the other sketch
   * @return this sketch
   * @throws NullPointerException if the argument is null
   */
  public ExaLogLog add(ExaLogLog other) {
    requireNonNull(other, "null argument");
    if (other.t != t) {
      throw new IllegalArgumentException(
          "merging of ExaLogLog sketches with different t-parameter is not possible");
    }
    if (other.d < d) {
      throw new IllegalArgumentException("other has smaller d-parameter");
    }
    if (other.p < p) {
      throw new IllegalArgumentException("other has smaller precision");
    }
    final int m = getNumRegisters(p);
    PackedArrayHandler handler = getPackedArrayHandler();
    if (other.d == d && other.p == p) {
      // fast path if register parameters are equal and no downsizing is needed
      for (int registerIndex = 0; registerIndex < m; ++registerIndex) {
        long thisR = handler.get(state, registerIndex);
        long otherR = handler.get(other.state, registerIndex);
        long mergedR = mergeRegister(thisR, otherR, d);
        if (thisR != mergedR) {
          handler.set(state, registerIndex, mergedR);
        }
      }
    } else {
      PackedArrayHandler otherHandler = other.getPackedArrayHandler();
      final int maxSubIndex = 1 << (other.p - p);
      final long downsizeThresholdU = computeDownsizeThresholdU(t, other.p);
      for (int registerIndex = 0; registerIndex < m; ++registerIndex) {
        long mergedR =
            downsizeRegister(
                otherHandler.get(other.state, registerIndex),
                t,
                other.d,
                d,
                other.p,
                p,
                0,
                downsizeThresholdU);
        for (int subIndex = 1; subIndex < maxSubIndex; ++subIndex) {
          long otherR =
              downsizeRegister(
                  otherHandler.get(other.state, registerIndex + (subIndex << p)),
                  t,
                  other.d,
                  d,
                  other.p,
                  p,
                  subIndex,
                  downsizeThresholdU);
          mergedR = mergeRegister(mergedR, otherR, d);
        }
        if (mergedR != 0) {
          final long thisR = handler.get(state, registerIndex);
          mergedR = mergeRegister(mergedR, thisR, d);
          if (thisR != mergedR) {
            handler.set(state, registerIndex, mergedR);
          }
        }
      }
    }
    return this;
  }

  /**
   * Returns a downsized copy of this sketch with a precision that is not larger than the given
   * precision parameter.
   *
   * @param d the d-parameter used for downsizing
   * @param p the precision parameter used for downsizing
   * @return the downsized copy
   * @throws IllegalArgumentException if the precision parameter is invalid
   */
  public ExaLogLog downsize(int d, int p) {
    checkPrecisionParameter(p, getMinP(), getMaxP(t));
    checkDParameter(d, t);
    if (p >= this.p && d >= this.d) {
      return copy();
    } else {
      return create(t, d, p).add(this);
    }
  }

  /**
   * Resets this sketch to its initial state representing an empty set.
   *
   * @return this sketch
   */
  public ExaLogLog reset() {
    Arrays.fill(state, (byte) 0);
    return this;
  }

  /**
   * Returns a reference to the internal state of this sketch.
   *
   * <p>The returned state is never {@code null}.
   *
   * @return the internal state of this sketch
   */
  public byte[] getState() {
    return state;
  }

  /**
   * Returns the precision parameter of this sketch.
   *
   * @return the precision parameter
   */
  public int getP() {
    return p;
  }

  /**
   * Adds a new element represented by a 64-bit hash value to this sketch and passes, if the
   * internal state has changed, decrements of the state change probability to the given {@link
   * StateChangeObserver}.
   *
   * <p>In order to get good estimates, it is important that the hash value is calculated using a
   * high-quality hash algorithm.
   *
   * @param hashValue a 64-bit hash value
   * @param martingaleEstimator a martingale estimator
   * @return this sketch
   */
  public ExaLogLog add(long hashValue, MartingaleEstimator martingaleEstimator) {
    long mask = ((1L << t) << p) - 1;
    int idx = (int) ((hashValue & mask) >>> t);
    int nlz = Long.numberOfLeadingZeros(hashValue | mask); // in {0, 1, ..., 64-p-t}
    long k = ((long) nlz << t) + (hashValue & ((1L << t) - 1)) + 1; // in [1, (65 - p - t) * 2^t]
    PackedArrayHandler registerAccess = getPackedArrayHandler();
    long rOld = registerAccess.get(state, idx);
    long u = rOld >>> d;
    long delta = k - u;
    if (delta > 0) {
      long rNew = k << d;
      if (delta <= d) {
        rNew |= ((1L << d) | (rOld & ((1L << d) - 1))) >>> delta;
      }
      registerAccess.set(state, idx, rNew);
      if (martingaleEstimator != null) {
        martingaleEstimator.decrementStateChangeProbability(
            (getRegisterChangeProbabilityScaled(rOld) - getRegisterChangeProbabilityScaled(rNew))
                * 0x1p-64);
      }
    } else {
      if (delta < 0 && d + delta >= 0) {
        long rNew = rOld;
        rNew |= (1L << (d + delta));
        if (rNew != rOld) {
          registerAccess.set(state, idx, rNew);
          if (martingaleEstimator != null) {
            int q = 63 - t - p;
            martingaleEstimator.decrementStateChangeProbability(pow2(Math.max(q - nlz, 0) - 64));
          }
        }
      }
    }
    return this;
  }

  /**
   * Adds a new element, represented by a 32-bit token obtained from {@code computeToken(long)}, to
   * this sketch and updates the given {@link MartingaleEstimator}.
   *
   * <p>{@code addToken(computeToken(hash), martingaleEstimator)} is equivalent to {@code add(hash,
   * martingaleEstimator)}
   *
   * @param token a 32-bit hash token
   * @param martingaleEstimator a martingale estimator
   * @return this sketch
   */
  public ExaLogLog addToken(int token, MartingaleEstimator martingaleEstimator) {
    return add(DistinctCountUtil.reconstructHash(token, V), martingaleEstimator);
  }

  /**
   * Returns the probability of an internal state change when a new distinct element is added.
   *
   * @return the state change probability
   */
  public double getStateChangeProbability() {
    int m = getNumRegisters(p);
    PackedArrayHandler registerAccess = getPackedArrayHandler();
    long first = getRegisterChangeProbabilityScaled(registerAccess.get(state, 0));
    long sum = first;
    for (int idx = 1; idx < m; ++idx) {
      sum += getRegisterChangeProbabilityScaled(registerAccess.get(state, idx));
    }
    // the sum can be zero because of two reasons:
    // 1) all registers are saturated and have therefore zero change probability yielding zero
    // overall change probability
    // 2) all registers are in the initial state and have a change probability of 1. The scaled
    // change probability would be 2^(64 - p). Summing up over all 2^p registers would give 2^64
    // which leads to a long overflow. This case needs special handling and an overall change
    // probability of 1 needs to be returned.
    if (sum != 0 || first == 0) {
      return DistinctCountUtil.unsignedLongToDouble(sum) * 0x1p-64;
    } else {
      return 1.;
    }
  }

  // register change probability multiplied by 2^(64 - p)
  private long getRegisterChangeProbabilityScaled(long r) {
    return contribute(r, null, t, d, p);
  }

  // computation of ML equation coefficients
  static long contribute(long r, int[] b, int t, int d, int p) {
    int u = (int) (r >>> d);
    if (u == 0) return 1L << -p;
    int q = 63 - t - p;
    int j = (u - 1) >>> t;
    int i = Math.min(q, j);
    long rInv = ~r;
    int numBits = (u - 1) - (i << t);
    long mask = 0xFFFFFFFFFFFFFFFFL << Math.max(0, d - numBits);
    long mask2 = mask & ((1L << d) - 1);
    long a = (((i + 2L) << t) - u + Long.bitCount(rInv & mask2)) << (q - i);
    if (b != null) b[i] += 1 + Long.bitCount(r & mask2);
    if (t <= 5) {
      int shift = 1 << t;
      mask ^= (mask >> shift);
      while (i > 0 && mask != 0) {
        i -= 1;
        a += (long) Long.bitCount(mask & rInv) << (q - i);
        if (b != null) b[i] += Long.bitCount(mask & r);
        mask >>>= shift;
      }
    } else if (i > 0) {
      mask = ~mask;
      i -= 1;
      a += (long) Long.bitCount(mask & rInv) << (q - i);
      if (b != null) b[i] += Long.bitCount(mask & r);
    }
    return a;
  }

  double getDistinctCountEstimate(SolverStatistics solverStatistics) {
    int m = getNumRegisters(p);

    long agg = 0;
    int[] b = new int[64];
    PackedArrayHandler registerAccess = getPackedArrayHandler();
    for (int idx = 0; idx < m; idx += 1) {
      agg += contribute(registerAccess.get(state, idx), b, t, d, p);
    }
    if (agg == 0) {
      // agg can be zero because of two reasons:
      // 1) all registers are saturated, which implies b[63 - t - p] to be nonzero -> estimate is
      // infinite
      // 2) all registers are zero (initial state) -> estimate is zero
      return (b[63 - t - p] == 0) ? 0 : Double.POSITIVE_INFINITY;
    }

    double factor = m << (t + 1);
    double a = unsignedLongToDouble(agg) * 0x1p-64 * factor;

    return factor
        * DistinctCountUtil.solveMaximumLikelihoodEquation(a, b, 63 - p - t, 0., solverStatistics)
        / (1 + ML_BIAS_CORRECTION_CONSTANTS[t][d] / m);
  }
}
