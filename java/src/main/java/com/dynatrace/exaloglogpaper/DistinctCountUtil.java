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

import static java.util.Objects.requireNonNull;

class DistinctCountUtil {

  private DistinctCountUtil() {}

  static IllegalArgumentException getUnexpectedStateLengthException() {
    return new IllegalArgumentException("unexpected state length!");
  }

  /**
   * Maximizes the expression
   *
   * <p>{@code e^{-x*a} * (1 - e^{-x})^b[0] * (1 - e^{-x/2})^b[1] * (1 - e^{-x/2^2})^b[2] * ... * (1
   * - e^{-x/2^n})^b[n]}
   *
   * <p>where {@code a} and all elements of {@code b} must be nonnegative. If this is not the case,
   * or if neither {@code a} nor any element in {@code b} is positive, or if {@code n >= b.length}
   * or {@code n >= 64} the behavior of this function is not defined. {@code a} must be either zero
   * or greater than or equal to 2^{-64}.
   *
   * @param a parameter a
   * @param b parameter b
   * @param n parameter n
   * @param relativeErrorLimit the relative error limit
   * @return the value that maximizes the expression
   * @throws RuntimeException if n >= b.length
   */
  static double solveMaximumLikelihoodEquation(
      double a, int[] b, int n, double relativeErrorLimit) {
    return solveMaximumLikelihoodEquation(a, b, n, relativeErrorLimit, null);
  }

  static class SolverStatistics {
    int iterationCounter;
  }

  static double solveMaximumLikelihoodEquation(
      double a, int[] b, int n, double relativeErrorLimit, SolverStatistics solverStatistics) {

    long sigma0 = 0;
    double sigma1 = 0;
    int uMin = -1;
    int uMax = 0;
    for (int j = 0; j <= n; ++j) {
      int bj = b[j];
      if (bj > 0) {
        if (uMin < 0) uMin = j;
        uMax = j;
        sigma0 += bj;
        sigma1 += Double.longBitsToDouble(Double.doubleToRawLongBits(bj) - ((long) j << 52));
      }
    }
    if (uMin < 0) return 0;

    final double powUMax = pow2(uMax);
    sigma1 *= powUMax;
    final double aPow = a * powUMax;
    double x = sigma1 / aPow;

    if (uMin < uMax) {
      x = Math.expm1(Math.log1p(x) * (sigma0 / sigma1));

      while (true) { // Newton iteration
        if (solverStatistics != null) solverStatistics.iterationCounter += 1;
        double lambda = 1;
        double eta = 0;
        double phi = b[uMax];
        double psi = 0;

        double y = x; // x could be +inf, if a was 0
        int u = uMax;
        while (true) {
          u -= 1;
          double yPlus2 = 2. + y; // is +inf, if x = +inf
          double z = 2. / yPlus2; // is always in range [0,1], is 0, if x = +inf
          lambda *= z; // is 0, if x = +inf
          eta =
              eta * (2. - z)
                  + (1.
                      - z); // eta is increasing and will never overflow as the number of iterations
          // is limited, eta <= 2^(uMax-u+1)-1
          double t = b[u] * lambda;
          phi += t;
          psi += t * eta;
          if (u <= uMin) break;
          y *= yPlus2;
        }

        double xPrime = aPow * x;
        if (!(phi > xPrime)) break;
        double eps = (phi - xPrime) / (psi + xPrime);
        double oldX = x;
        x += x * eps;
        if (eps <= relativeErrorLimit || !(x > oldX)) break;
      }
    }
    return Math.log1p(x) * powUMax;
  }

  static int computeToken(long hashValue, int v) {
    long mask = 0xFFFFFFFFFFFFFFFFL >>> -v;
    int idx = (int) (hashValue & mask);
    int nlz = Long.numberOfLeadingZeros(hashValue | mask);
    return (idx << 6) | nlz;
  }

  static long reconstructHash(int token, int v) {
    long idx = token >>> 6;
    return ((0xFFFFFFFFFFFFFFFFL >>> v >>> token) << v) | idx;
  }

  static final int V_MAX = 26; // 32 - 6
  static final int V_MIN = 1;

  /**
   * An iterable over hash tokens.
   *
   * <p>A (v+6)-bit hash token is computed from a 64-bit hash value. It stores v bits of the hash
   * value in the upper 26 bits. The remaining 6 bits are used to store the number of leading zeros
   * of the remaining (64 - v) bits of the hash value, which can be in the range [0, 64 - v]. Here v
   * denotes the token parameter which must be in the range [1,26].
   *
   * <p>Implementations of this interface must ensure that the iteration over tokens is ordered,
   * which means that tokens with the same most significant bits are output one after the other and
   * not interleaved with tokens with different most significant bits. However, it is allowed to
   * output invalid tokens at any time as it is expected that they are ignored during later
   * processing. See {@link #isValidToken(int, int)} for a definition of a valid token.
   */
  interface TokenIterable {

    /**
     * Returns a token iterator.
     *
     * @return a token iterator
     */
    TokenIterator iterator();
  }

  /** A token iterator. */
  interface TokenIterator {

    /**
     * Returns true if the iteration has more tokens.
     *
     * @return true if the iteration has more tokens
     */
    boolean hasNext();

    /**
     * Returns the next token.
     *
     * <p>Invalid token may be returned, which can be identified using {@link #isValidToken(int,
     * int)}. Invalid tokens are expected to be ignored during further processing.
     *
     * @return the next token
     */
    int nextToken();
  }

  /**
   * Returns {@code true}, if the token is valid.
   *
   * <p>A token is valid if the (unsigned) value is in the range of [0, 2^(6+v)-1] and the least
   * significant 6 bits does not exceed (64 - v) where v denotes the token parameter.
   *
   * @param token the token
   * @param token the token parameter, must be in the range [1,26]
   * @return true, if the token is valid
   */
  static boolean isValidToken(int token, int tokenParameter) {
    int nlz = token & 0x3f;
    return ((token >>> 6 >>> tokenParameter) == 0) && (nlz <= 64 - tokenParameter);
  }

  private static final int INVALID_TOKEN_INDEX = 0xFFFFFFFF;

  /**
   * Estimates the distinct count from a sorted list of tokens.
   *
   * @param tokenIterable an iterable over the sorted list of tokens
   * @return the estimated distinct count
   */
  static double estimateDistinctCountFromSortedTokens(
      TokenIterable tokenIterable, int tokenParameter) {
    return estimateDistinctCountFromSortedTokens(tokenIterable, tokenParameter, null);
  }

  static double estimateDistinctCountFromSortedTokens(
      TokenIterable tokenIterable, int tokenParameter, SolverStatistics solverStatistics) {
    requireNonNull(tokenIterable);

    TokenIterator tokenIterator = tokenIterable.iterator();

    int maxNlzInTokenMinus1 = 63 - tokenParameter;
    long z = 1L << maxNlzInTokenMinus1;

    long a = 0; // corresponds to 2^64
    int[] b = new int[63];

    int currentIdx = INVALID_TOKEN_INDEX;
    long currentFlags = 0;
    int maxNonZeroIndex = -1;
    while (tokenIterator.hasNext()) {
      int token = tokenIterator.nextToken();
      if (!isValidToken(token, tokenParameter)) continue;
      int idx = token >>> 6;
      if (currentIdx != idx) {
        currentFlags = 0;
        currentIdx = idx;
      }
      long mask = (1L << token);
      if ((currentFlags & mask) == 0L) {
        currentFlags |= mask;
        int j = Math.min(token & 0x3f, maxNlzInTokenMinus1);
        b[j] += 1;
        a -= z >>> j;
        if (j > maxNonZeroIndex) maxNonZeroIndex = j;
      }
    }

    if (maxNonZeroIndex < 0) {
      // implies that all b[i] are zero
      return 0;
    }
    return DistinctCountUtil.solveMaximumLikelihoodEquation(
            unsignedLongToDouble(a) * pow2(-maxNlzInTokenMinus1),
            b,
            maxNonZeroIndex,
            0.,
            solverStatistics)
        * pow2(tokenParameter + 1);
  }

  static double unsignedLongToDouble(long l) {
    double d = l & 0x7fffffffffffffffL;
    if (l < 0) d += 0x1p63;
    return d;
  }

  static double pow2(int x) {
    return Double.longBitsToDouble((x + 1023L) << 52);
  }
}
