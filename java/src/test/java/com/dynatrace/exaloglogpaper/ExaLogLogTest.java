//
// Copyright (c) 2024 Dynatrace LLC. All rights reserved.
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

import static com.dynatrace.exaloglogpaper.ExaLogLog.*;
import static java.lang.Math.pow;
import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.assertj.core.data.Percentage.withPercentage;

import com.dynatrace.exaloglogpaper.TestUtils.HashGenerator;
import com.dynatrace.hash4j.hashing.HashStream64;
import com.dynatrace.hash4j.hashing.Hashing;
import com.dynatrace.hash4j.random.PseudoRandomGenerator;
import com.dynatrace.hash4j.random.PseudoRandomGeneratorProvider;
import com.dynatrace.hash4j.util.PackedArray;
import com.google.common.collect.Sets;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.DoubleUnaryOperator;
import java.util.function.IntToDoubleFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.zip.Deflater;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public abstract class ExaLogLogTest {

  protected abstract ExaLogLog create(int p);

  protected abstract long getCompatibilityFingerPrint();

  protected final int getStateLength(int p) {
    return ((getBitsPerRegister(p) << p) + (Byte.SIZE - 1)) / Byte.SIZE;
  }

  protected abstract ExaLogLog wrap(byte[] state);

  protected PackedArray.PackedArrayHandler getHandler(int p) {
    return PackedArray.getHandler(getBitsPerRegister(p));
  }

  @Test
  void testCheckPrecisionParameter() {
    assertThatIllegalArgumentException().isThrownBy(() -> checkPrecisionParameter(1, 3, 5));
    assertThatIllegalArgumentException().isThrownBy(() -> checkPrecisionParameter(2, 3, 5));
    assertThatNoException().isThrownBy(() -> checkPrecisionParameter(3, 3, 5));
    assertThatNoException().isThrownBy(() -> checkPrecisionParameter(4, 3, 5));
    assertThatNoException().isThrownBy(() -> checkPrecisionParameter(5, 3, 5));
    assertThatIllegalArgumentException().isThrownBy(() -> checkPrecisionParameter(6, 3, 5));
    assertThatIllegalArgumentException().isThrownBy(() -> checkPrecisionParameter(7, 3, 5));
  }

  @Test
  void testStateCompatibility() {
    PseudoRandomGenerator pseudoRandomGenerator =
        PseudoRandomGeneratorProvider.splitMix64_V1().create();
    HashStream64 hashStream = Hashing.komihash4_3().hashStream();
    long[] cardinalities = {1, 10, 100, 1000, 10000, 100000};
    int numCycles = 10;
    for (int p = getMinP(); p <= getMaxP(getT()); ++p) {
      for (long cardinality : cardinalities) {
        for (int i = 0; i < numCycles; ++i) {
          ExaLogLog sketch = create(p);
          for (long c = 0; c < cardinality; ++c) {
            sketch.add(pseudoRandomGenerator.nextLong());
          }
          hashStream.putByteArray(sketch.getState());
        }
      }
    }
    assertThat(hashStream.getAsLong()).isEqualTo(getCompatibilityFingerPrint());
  }

  @Test
  void testEmptyMerge() {
    ExaLogLog sketch1 = create(12);
    ExaLogLog sketch2 = create(12);
    ExaLogLog sketchMerged = merge(sketch1, sketch2);
    assertThat(sketchMerged.getDistinctCountEstimate()).isZero();
  }

  @Test
  void testReset() {
    ExaLogLog sketch = create(12);
    SplittableRandom random = new SplittableRandom(0L);
    for (int i = 0; i < 10000; ++i) {
      sketch.add(random.nextLong());
    }
    assertThat(sketch.getDistinctCountEstimate()).isGreaterThan(1000);
    sketch.reset();
    assertThat(sketch.getDistinctCountEstimate()).isZero();
    assertThat(sketch.getState()).containsOnly(0);
  }

  private void testAddAndMerge(
      int p1, long distinctCount1, int p2, long distinctCount2, long seed) {
    ExaLogLog sketch1a = create(p1);
    ExaLogLog sketch2a = create(p2);
    ExaLogLog sketch1b = create(p1);
    ExaLogLog sketch2b = create(p2);
    ExaLogLog sketchTotal = create(Math.min(p1, p2));
    SplittableRandom random = new SplittableRandom(seed);
    for (long i = 0; i < distinctCount1; ++i) {
      long hashValue = random.nextLong();
      sketch1a.add(hashValue);
      sketch1b.add(hashValue);
      sketchTotal.add(hashValue);
    }
    for (long i = 0; i < distinctCount2; ++i) {
      long hashValue = random.nextLong();
      sketch2a.add(hashValue);
      sketch2b.add(hashValue);
      sketchTotal.add(hashValue);
    }
    assertThat(merge(sketch1a, sketch2a).getState()).isEqualTo(sketchTotal.getState());
    if (p1 < p2) {
      sketch1a.add(sketch2a);
      assertThatIllegalArgumentException().isThrownBy(() -> sketch2b.add(sketch1b));
      assertThat(sketch1a.getState()).isEqualTo(sketchTotal.getState());
    } else if (p1 > p2) {
      sketch2a.add(sketch1a);
      assertThatIllegalArgumentException().isThrownBy(() -> sketch1b.add(sketch2b));
      assertThat(sketch2a.getState()).isEqualTo(sketchTotal.getState());
    } else {
      sketch1a.add(sketch2a);
      sketch2b.add(sketch1b);
      assertThat(sketch1a.getState()).isEqualTo(sketchTotal.getState());
      assertThat(sketch2b.getState()).isEqualTo(sketchTotal.getState());
    }
  }

  @Test
  void testAddAndMerge() {
    SplittableRandom random = new SplittableRandom(0x11a73f21bb8ad8f6L);
    int[] pVals = IntStream.range(getMinP(), Math.min(12, getMaxP(getT()))).toArray();
    long[] distinctCounts = {
      0, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384
    };
    for (int p1 : pVals) {
      for (int p2 : pVals) {
        for (long distinctCount1 : distinctCounts) {
          for (long distinctCount2 : distinctCounts) {
            testAddAndMerge(p1, distinctCount1, p2, distinctCount2, random.nextLong());
          }
        }
      }
    }
  }

  private void testAddAndMerge2(int p1, int p2, long seed) {
    ExaLogLog sketch1a = create(p1);
    ExaLogLog sketch2a = create(p2);
    ExaLogLog sketch1b = create(p1);
    ExaLogLog sketch2b = create(p2);
    ExaLogLog sketchTotal = create(Math.min(p1, p2));

    SplittableRandom random = new SplittableRandom(seed);

    int maxP = Math.max(p1, p2);
    List<HashGenerator> hashGenerators = getHashGenerators(maxP);
    long[] hashPool = new long[(1 << maxP) * hashGenerators.size()];
    int c = 0;
    for (HashGenerator hashGenerator : hashGenerators) {
      for (int idx = 0; idx < (1 << maxP); ++idx) {
        hashPool[c] = hashGenerator.generateHashValue(idx);
        c += 1;
      }
    }

    for (int i = 0; i < (int) (hashPool.length * 0.3); ++i) {
      long hashValue = hashPool[random.nextInt(0, hashPool.length)];
      sketch1a.add(hashValue);
      sketch1b.add(hashValue);
      sketchTotal.add(hashValue);
    }

    for (int i = 0; i < (int) (hashPool.length * 0.2); ++i) {
      long hashValue = hashPool[random.nextInt(0, hashPool.length)];
      sketch2a.add(hashValue);
      sketch2b.add(hashValue);
      sketchTotal.add(hashValue);
    }

    assertThat(merge(sketch1a, sketch2a).getState()).isEqualTo(sketchTotal.getState());
    if (p1 < p2) {
      sketch1a.add(sketch2a);
      assertThatIllegalArgumentException().isThrownBy(() -> sketch2b.add(sketch1b));
      assertThat(sketch1a.getState()).isEqualTo(sketchTotal.getState());
    } else if (p1 > p2) {
      sketch2a.add(sketch1a);
      assertThatIllegalArgumentException().isThrownBy(() -> sketch1b.add(sketch2b));
      assertThat(sketch2a.getState()).isEqualTo(sketchTotal.getState());
    } else {
      sketch1a.add(sketch2a);
      sketch2b.add(sketch1b);
      assertThat(sketch1a.getState()).isEqualTo(sketchTotal.getState());
      assertThat(sketch2b.getState()).isEqualTo(sketchTotal.getState());
    }
  }

  @Test
  void testAddAndMerge2() {
    int numIterations = 100;
    SplittableRandom random = new SplittableRandom(0x441c8be98996e470L);
    int[] pVals = IntStream.range(getMinP(), Math.min(12, getMaxP(getT()))).toArray();
    for (int p1 : pVals) {
      for (int p2 : pVals) {
        for (int i = 0; i < numIterations; ++i) {
          testAddAndMerge2(p1, p2, random.nextLong());
        }
      }
    }
  }

  private static int compressedLengthInBytes(byte[] data, byte[] work) {
    Deflater deflater = new Deflater(Deflater.BEST_COMPRESSION);
    deflater.setInput(data);
    deflater.finish();
    int numBytes = deflater.deflate(work);
    assertThat(numBytes).isLessThan(work.length);
    assertThat(deflater.finished()).isTrue();
    return numBytes;
  }

  @Test
  void testCompressedMemoryVarianceProduct() {
    int numCycles = 100;
    long trueDistinctCount = 1000000;
    int p = 12;
    long sumCompressedSizeInBytes = 0;
    long sumUncompressedSizeInBytes = 0;

    SplittableRandom random = new SplittableRandom(0L);
    ExaLogLog sketch = create(p);
    byte[] work = new byte[getStateLength(p) * 3];
    for (int i = 0; i < numCycles; ++i) {
      sketch.reset();
      for (long k = 0; k < trueDistinctCount; ++k) {
        long hash = random.nextLong();
        sketch.add(hash);
      }
      int uncompressedSizeInBytes = sketch.getState().length;
      sumCompressedSizeInBytes += compressedLengthInBytes(sketch.getState(), work);
      sumUncompressedSizeInBytes += uncompressedSizeInBytes;
    }

    double expectedVariance =
        pow(PrecomputedConstants.getTheoreticalRelativeErrorML(getT(), getD(), p), 2);

    double memoryVarianceProduct =
        Byte.SIZE * sumCompressedSizeInBytes * expectedVariance / numCycles;

    double theoreticalCompressedMemoryVarianceProduct =
        PrecomputedConstants.getCompressedMvpML(getT(), getD());
    assertThat(memoryVarianceProduct)
        .isBetween(
            theoreticalCompressedMemoryVarianceProduct,
            theoreticalCompressedMemoryVarianceProduct * 1.5);
    assertThat((double) sumCompressedSizeInBytes)
        .isLessThanOrEqualTo(sumUncompressedSizeInBytes * 0.9);
  }

  @Test
  void testDownsizeIllegalArguments() {
    ExaLogLog sketch = create(8);
    for (int p = getMinP() - 100; p < getMaxP(getT()) + 100; ++p) {
      int pFinal = p;
      if (p >= getMinP() && p <= getMaxP(getT())) {
        assertThatNoException().isThrownBy(() -> sketch.downsize(getD(), pFinal));
      } else {
        assertThatIllegalArgumentException().isThrownBy(() -> sketch.downsize(getD(), pFinal));
      }
    }
  }

  @Test
  void testCreateIllegalArguments() {
    for (int p = getMinP() - 100; p < getMaxP(getT()) + 100; ++p) {
      int pFinal = p;
      if (p >= getMinP() && p <= getMaxP(getT())) {
        assertThatNoException().isThrownBy(() -> create(pFinal));
      } else {
        assertThatIllegalArgumentException().isThrownBy(() -> create(pFinal));
      }
    }
  }

  private void testDownsize(int pOriginal, int pDownsized, long distinctCount, long seed) {
    SplittableRandom random = new SplittableRandom(seed);
    ExaLogLog sketchOrig = create(pOriginal);
    ExaLogLog sketchDownsized = create(pDownsized);

    for (long i = 0; i < distinctCount; ++i) {
      long hashValue = random.nextLong();
      sketchOrig.add(hashValue);
      sketchDownsized.add(hashValue);
    }
    assertThat(sketchOrig.downsize(getD(), pDownsized).getState())
        .isEqualTo(sketchDownsized.getState());
  }

  @Test
  void testDownsize() {
    SplittableRandom random = new SplittableRandom(0x237846c7b27df6b4L);
    int[] pVals = IntStream.range(getMinP(), 16).toArray();
    long[] distinctCounts = {
      0, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384
    };
    for (int pOriginalIdx = 0; pOriginalIdx < pVals.length; ++pOriginalIdx) {
      for (int pDownsizedIdx = 0; pDownsizedIdx <= pOriginalIdx; ++pDownsizedIdx) {
        for (long distinctCount : distinctCounts) {
          testDownsize(pVals[pOriginalIdx], pVals[pDownsizedIdx], distinctCount, random.nextLong());
        }
      }
    }
  }

  protected void testDistinctCountEstimation(
      int p,
      long seed,
      long[] distinctCounts,
      List<ExaLogLog.Estimator> estimators,
      List<IntToDoubleFunction> pToTheoreticalRelativeStandardErrorFunctions,
      double[] relativeBiasThresholds,
      double[] relativeRmseThresholds,
      double[] asymptoticThresholds,
      double relativeBiasThresholdMartingale,
      double relativeRmseThresholdMartingale,
      double asymptoticThresholdMartingale,
      ExaLogLog.Estimator defaultEstimator) {

    int numIterations = 1000;
    SplittableRandom random = new SplittableRandom(seed);

    assertThat(estimators.size())
        .isEqualTo(relativeBiasThresholds.length)
        .isEqualTo(relativeRmseThresholds.length)
        .isEqualTo(asymptoticThresholds.length)
        .isEqualTo(pToTheoreticalRelativeStandardErrorFunctions.size());
    int numEstimators = estimators.size();

    double[] theoreticalRelativeStandardErrors = new double[numEstimators + 1];
    double[][] estimationErrorsMoment1 = new double[numEstimators + 1][];
    double[][] estimationErrorsMoment2 = new double[numEstimators + 1][];
    for (int i = 0; i < numEstimators + 1; ++i) {
      estimationErrorsMoment1[i] = new double[distinctCounts.length];
      estimationErrorsMoment2[i] = new double[distinctCounts.length];
    }

    for (int i = 0; i < numEstimators; ++i) {
      theoreticalRelativeStandardErrors[i] =
          pToTheoreticalRelativeStandardErrorFunctions.get(i).applyAsDouble(p);
    }
    theoreticalRelativeStandardErrors[numEstimators] =
        PrecomputedConstants.getTheoreticalRelativeErrorMartingale(getT(), getD(), p);

    for (int i = 0; i < numIterations; ++i) {
      ExaLogLog sketch = create(p);
      ExaLogLog sketchMartingale = create(p);
      MartingaleEstimator martingaleEstimator = new MartingaleEstimator();
      long trueDistinctCount = 0;
      int distinctCountIndex = 0;
      while (distinctCountIndex < distinctCounts.length) {
        if (trueDistinctCount == distinctCounts[distinctCountIndex]) {
          for (int estimatorIdx = 0; estimatorIdx < numEstimators; ++estimatorIdx) {
            ExaLogLog.Estimator estimator = estimators.get(estimatorIdx);
            double estimate = sketch.getDistinctCountEstimate(estimator);
            if (defaultEstimator.equals(estimator)) {
              assertThat(sketch.getDistinctCountEstimate(defaultEstimator)).isEqualTo(estimate);
            }
            double distinctCountEstimationError = estimate - trueDistinctCount;
            estimationErrorsMoment1[estimatorIdx][distinctCountIndex] +=
                distinctCountEstimationError;
            estimationErrorsMoment2[estimatorIdx][distinctCountIndex] +=
                distinctCountEstimationError * distinctCountEstimationError;
          }

          double distinctCountEstimationErrorMartingale =
              martingaleEstimator.getDistinctCountEstimate() - trueDistinctCount;
          estimationErrorsMoment1[numEstimators][distinctCountIndex] +=
              distinctCountEstimationErrorMartingale;
          estimationErrorsMoment2[numEstimators][distinctCountIndex] +=
              distinctCountEstimationErrorMartingale * distinctCountEstimationErrorMartingale;

          distinctCountIndex += 1;

          assertThat(sketch.getState()).isEqualTo(sketchMartingale.getState());
          assertThat(sketch.getStateChangeProbability())
              .isEqualTo(martingaleEstimator.getStateChangeProbability());
        }
        long hash = random.nextLong();
        sketch.add(hash);
        sketchMartingale.add(hash, martingaleEstimator);
        trueDistinctCount += 1;
      }
    }

    for (int distinctCountIndex = 0;
        distinctCountIndex < distinctCounts.length;
        ++distinctCountIndex) {
      long trueDistinctCount = distinctCounts[distinctCountIndex];

      for (int estimatorIdx = 0; estimatorIdx < numEstimators; ++estimatorIdx) {
        double relativeBias =
            estimationErrorsMoment1[estimatorIdx][distinctCountIndex]
                / (trueDistinctCount
                    * (double) numIterations
                    * theoreticalRelativeStandardErrors[estimatorIdx]);
        double relativeRmse =
            Math.sqrt(estimationErrorsMoment2[estimatorIdx][distinctCountIndex] / numIterations)
                / (trueDistinctCount * theoreticalRelativeStandardErrors[estimatorIdx]);

        double relativeBiasThreshold = relativeBiasThresholds[estimatorIdx];
        double relativeRmseThreshold = relativeRmseThresholds[estimatorIdx];
        double asymptoticThreshold = asymptoticThresholds[estimatorIdx];

        if (trueDistinctCount > 0) {
          // verify bias to be significantly smaller than standard error
          assertThat(Math.abs(relativeBias)).isLessThan(relativeBiasThreshold);
        }
        if (trueDistinctCount > 0) {
          // test if observed root mean square error is not much greater than relative standard
          // error
          assertThat(relativeRmse).isLessThan(relativeRmseThreshold);
        }
        if (trueDistinctCount > 10 * (1L << p)) {
          // test asymptotic behavior (distinct count is much greater than number of registers
          // (state size) given by (1 << p)
          // observed root mean square error should be approximately equal to the standard error
          assertThat(relativeRmse).isCloseTo(1., within(asymptoticThreshold));
        }
      }

      {
        double relativeBiasMartingale =
            estimationErrorsMoment1[numEstimators][distinctCountIndex]
                / (trueDistinctCount
                    * (double) numIterations
                    * theoreticalRelativeStandardErrors[numEstimators]);
        double relativeRmseMartingale =
            Math.sqrt(estimationErrorsMoment2[numEstimators][distinctCountIndex] / numIterations)
                / (trueDistinctCount * theoreticalRelativeStandardErrors[numEstimators]);

        if (trueDistinctCount > 0) {
          // verify bias to be significantly smaller than standard error
          // System.out.println(trueDistinctCount + " " + p);
          assertThat(Math.abs(relativeBiasMartingale)).isLessThan(relativeBiasThresholdMartingale);
        }
        if (trueDistinctCount > 0) {
          // test if observed root mean square error is not much greater than relative standard
          // error
          assertThat(relativeRmseMartingale).isLessThan(relativeRmseThresholdMartingale);
        }
        if (trueDistinctCount > 10 * (1L << p)) {
          // test asymptotic behavior (distinct count is much greater than number of registers
          // (state size) given by (1 << p)
          // observed root mean square error should be approximately equal to the standard error
          assertThat(relativeRmseMartingale).isCloseTo(1., within(asymptoticThresholdMartingale));
        }
      }
    }
  }

  protected void testLargeDistinctCountEstimation(
      int p,
      long seed,
      long distinctCount,
      List<ExaLogLog.Estimator> estimators,
      List<IntToDoubleFunction> pToTheoreticalRelativeStandardErrorFunctions,
      double relativeBiasThreshold,
      double relativeRmseThreshold) {

    int numIterations = 1000;

    int numEstimators = estimators.size();

    double[] theoreticalRelativeStandardErrors = new double[numEstimators + 1];
    double[] estimationErrorsMoment1 = new double[numEstimators + 1];
    double[] estimationErrorsMoment2 = new double[numEstimators + 1];

    for (int i = 0; i < numEstimators; ++i) {
      theoreticalRelativeStandardErrors[i] =
          pToTheoreticalRelativeStandardErrorFunctions.get(i).applyAsDouble(p);
    }
    theoreticalRelativeStandardErrors[numEstimators] =
        PrecomputedConstants.getTheoreticalRelativeErrorMartingale(getT(), getD(), p);

    List<HashGenerator> hashGenerators = getHashGenerators(p);
    TestUtils.Transition[] transitions = new TestUtils.Transition[hashGenerators.size() * (1 << p)];

    PseudoRandomGenerator prg = PseudoRandomGeneratorProvider.splitMix64_V1().create();
    prg.reset(seed);

    BigInt bigIntDistinctCount = BigInt.fromLong(distinctCount);

    ExaLogLog sketch = create(p);
    MartingaleEstimator martingaleEstimator = new MartingaleEstimator();
    for (int i = 0; i < numIterations; ++i) {
      TestUtils.generateTransitions(transitions, BigInt.createZero(), hashGenerators, p, prg);

      sketch.reset();
      martingaleEstimator.reset();

      int transitionIndex = 0;
      while (transitionIndex < transitions.length
          && transitions[transitionIndex].getDistinctCount().compareTo(bigIntDistinctCount) <= 0) {
        sketch.add(transitions[transitionIndex].getHash(), martingaleEstimator);
        transitionIndex += 1;
      }

      for (int estimatorIdx = 0; estimatorIdx < numEstimators; ++estimatorIdx) {
        ExaLogLog.Estimator estimator = estimators.get(estimatorIdx);
        double estimate = sketch.getDistinctCountEstimate(estimator);
        double distinctCountEstimationError = estimate - distinctCount;
        estimationErrorsMoment1[estimatorIdx] += distinctCountEstimationError;
        estimationErrorsMoment2[estimatorIdx] +=
            distinctCountEstimationError * distinctCountEstimationError;
      }
      {
        double estimate = martingaleEstimator.getDistinctCountEstimate();
        double distinctCountEstimationError = estimate - distinctCount;
        estimationErrorsMoment1[numEstimators] += distinctCountEstimationError;
        estimationErrorsMoment2[numEstimators] +=
            distinctCountEstimationError * distinctCountEstimationError;
      }
    }

    for (int estimatorIdx = 0; estimatorIdx <= numEstimators; ++estimatorIdx) {
      double relativeBias =
          Math.abs(estimationErrorsMoment1[estimatorIdx] / numIterations)
              / (distinctCount * theoreticalRelativeStandardErrors[estimatorIdx]);
      double relativeRmse =
          Math.sqrt(estimationErrorsMoment2[estimatorIdx] / numIterations)
              / (distinctCount * theoreticalRelativeStandardErrors[estimatorIdx]);

      assertThat(relativeBias).isCloseTo(0., within(relativeBiasThreshold));
      assertThat(relativeRmse).isCloseTo(1., within(relativeRmseThreshold));
    }
  }

  @Test
  void testEmpty() {
    for (int p = getMinP(); p <= getMaxP(getT()); ++p) {
      ExaLogLog sketch = create(p);
      assertThat(sketch.getDistinctCountEstimate()).isZero();
      for (ExaLogLog.Estimator estimator : getEstimators()) {
        assertThat(estimator.estimate(sketch)).isZero();
      }
      ExaLogLog mergedSketch = merge(sketch, sketch);
      assertThat(mergedSketch.getState()).isEqualTo(new byte[getStateLength(p)]);
      assertThat(mergedSketch.getDistinctCountEstimate()).isZero();
      for (ExaLogLog.Estimator estimator : getEstimators()) {
        assertThat(estimator.estimate(mergedSketch)).isZero();
      }
      assertThat(sketch.getStateChangeProbability()).isOne();
    }
  }

  @Test
  void testWrapZeros() {
    for (int p = getMinP(); p <= getMaxP(getT()); p += 1) {
      ExaLogLog sketch = wrap(new byte[getStateLength(p)]);
      assertThat(sketch.getDistinctCountEstimate()).isZero();
      for (ExaLogLog.Estimator estimator : getEstimators()) {
        assertThat(estimator.estimate(sketch)).isZero();
      }
    }
  }

  @Test
  void testRandomStates() {
    int numCycles = 10000;
    int minP = getMinP();
    int maxP = 8;
    SplittableRandom random = new SplittableRandom(0x822fa1dcf86f953eL);
    for (int i = 0; i < numCycles; ++i) {
      int p1 = random.nextInt(minP, maxP + 1);
      int p2 = random.nextInt(minP, maxP + 1);
      byte[] state1 = new byte[getStateLength(p1)];
      byte[] state2 = new byte[getStateLength(p2)];
      random.nextBytes(state1);
      random.nextBytes(state2);
      ExaLogLog sketch1 = wrap(state1);
      ExaLogLog sketch2 = wrap(state2);
      int newP1 = random.nextInt(minP, maxP + 1);
      int newP2 = random.nextInt(minP, maxP + 1);
      assertThatNoException().isThrownBy(sketch1::getDistinctCountEstimate);
      assertThatNoException().isThrownBy(sketch2::getDistinctCountEstimate);
      for (ExaLogLog.Estimator estimator : getEstimators()) {
        assertThatNoException().isThrownBy(() -> estimator.estimate(sketch1));
        assertThatNoException().isThrownBy(() -> estimator.estimate(sketch2));
      }
      assertThatNoException().isThrownBy(sketch1::copy);
      assertThatNoException().isThrownBy(sketch2::copy);
      assertThatNoException().isThrownBy(() -> sketch1.downsize(getD(), newP1));
      assertThatNoException().isThrownBy(() -> sketch2.downsize(getD(), newP2));
      assertThatNoException().isThrownBy(() -> merge(sketch1, sketch2));
      if (sketch1.getP() <= sketch2.getP()) {
        assertThatNoException().isThrownBy(() -> sketch1.add(sketch2));
      } else {
        assertThatNoException().isThrownBy(() -> sketch2.add(sketch1));
      }
    }
  }

  @Test
  void testDeduplication() {
    SplittableRandom random = new SplittableRandom(0x1dd4dbffe1c9f639L);
    int hashPoolSize = 100000;

    long[] hashValues = random.longs(hashPoolSize).toArray();
    for (int p = getMinP(); p <= Math.min(12, getMaxP(getT())); ++p) {
      ExaLogLog sketch1 = create(p);
      ExaLogLog sketch2 = create(p);
      Set<Long> insertedHashes = new HashSet<>();
      for (int i = 1; i <= hashPoolSize; ++i) {
        for (int k = 0; k < 3; ++k) {
          long hashValue = hashValues[random.nextInt(i)];
          sketch1.add(hashValue);
          if (insertedHashes.add(hashValue)) {
            sketch2.add(hashValue);
          }
        }
        assertThat(sketch1.getState()).isEqualTo(sketch2.getState());
      }
    }
  }

  @Test
  void testWrappingOfPotentiallyInvalidByteArrays() {
    for (int p = getMinP(); p <= getMaxP(getT()); ++p) {
      byte[] b = new byte[getStateLength(p)];
      int c = 0;
      while (c < 256) {
        for (int k = 0; k < b.length; ++k) {
          b[k] = (byte) c;
          c += 1;
        }
        assertThatNoException().isThrownBy(() -> wrap(b).getDistinctCountEstimate());
        for (ExaLogLog.Estimator estimator : getEstimators()) {
          assertThatNoException().isThrownBy(() -> estimator.estimate(wrap(b)));
        }
      }
    }
  }

  @Test
  void testWrapIllegalArguments() {
    Set<Integer> validLengths =
        IntStream.range(getMinP(), getMaxP(getT()) + 1)
            .map(this::getStateLength)
            .boxed()
            .collect(Collectors.toSet());
    Set<Integer> testLengths =
        IntStream.range(getMinP() - 1, getMaxP(getT()) + 2)
            .map(this::getStateLength)
            .flatMap(p -> IntStream.of(p - 3, p - 2, p - 1, p, p + 1, p + 2, p + 3))
            .boxed()
            .collect(Collectors.toCollection(HashSet::new));

    for (int len : validLengths) {
      assertThatNoException().isThrownBy(() -> wrap(new byte[len]));
    }

    for (int len : Sets.difference(testLengths, validLengths)) {
      assertThatIllegalArgumentException().isThrownBy(() -> wrap(new byte[len]));
    }

    assertThatNullPointerException().isThrownBy(() -> wrap(null));
  }

  protected void testErrorOfDistinctCountEqualOne(
      int[] pValues,
      ExaLogLog.Estimator estimator,
      IntToDoubleFunction pToTheoreticalRelativeStandardError,
      double[] relativeBiasLimit,
      double[] relativeRmseLimit) {

    testErrorOfDistinctCount(
        pValues,
        estimator,
        pToTheoreticalRelativeStandardError,
        relativeBiasLimit,
        relativeRmseLimit,
        this::calculateErrorOfDistinctCountEqualOne);
  }

  protected void testErrorOfDistinctCountEqualTwo(
      int[] pValues,
      ExaLogLog.Estimator estimator,
      IntToDoubleFunction pToTheoreticalRelativeStandardError,
      double[] relativeBiasLimit,
      double[] relativeRmseLimit) {

    testErrorOfDistinctCount(
        pValues,
        estimator,
        pToTheoreticalRelativeStandardError,
        relativeBiasLimit,
        relativeRmseLimit,
        this::calculateErrorOfDistinctCountEqualTwo);
  }

  protected void testErrorOfDistinctCountEqualThree(
      int[] pValues,
      ExaLogLog.Estimator estimator,
      IntToDoubleFunction pToTheoreticalRelativeStandardError,
      double[] relativeBiasLimit,
      double[] relativeRmseLimit) {

    testErrorOfDistinctCount(
        pValues,
        estimator,
        pToTheoreticalRelativeStandardError,
        relativeBiasLimit,
        relativeRmseLimit,
        this::calculateErrorOfDistinctCountEqualThree);
  }

  private void testErrorOfDistinctCount(
      int[] pValues,
      ExaLogLog.Estimator estimator,
      IntToDoubleFunction pToTheoreticalRelativeStandardError,
      double[] relativeBiasLimit,
      double[] relativeRmseLimit,
      BiFunction<Integer, ExaLogLog.Estimator, double[]> errorCalculator) {

    double[] relativeBiasValues = new double[pValues.length];
    double[] relativeRmseValues = new double[pValues.length];
    for (int i = 0; i < pValues.length; ++i) {
      double[] r = errorCalculator.apply(pValues[i], estimator);
      double bias = r[0];
      double rmse = r[1];
      double theoreticalRelativeError =
          pToTheoreticalRelativeStandardError.applyAsDouble(pValues[i]);
      relativeBiasValues[i] = bias / theoreticalRelativeError;
      relativeRmseValues[i] = rmse / theoreticalRelativeError;
    }

    DoubleUnaryOperator limitCalculator =
        x -> {
          BigDecimal bd = BigDecimal.valueOf(Math.abs(x) * 1.01);
          return bd.setScale(4, RoundingMode.UP).doubleValue();
        };
    double[] proposedBiasLimits = Arrays.stream(relativeBiasValues).map(limitCalculator).toArray();
    double[] proposedRmseLimits = Arrays.stream(relativeRmseValues).map(limitCalculator).toArray();

    String description =
        "proposed bias limits: "
            + Arrays.toString(proposedBiasLimits)
            + '\n'
            + "proposed rmse limits: "
            + Arrays.toString(proposedRmseLimits);

    assertThat(relativeBiasLimit).describedAs(description).hasSize(relativeBiasValues.length);
    for (int i = 0; i < pValues.length; ++i) {
      assertThat(relativeBiasValues[i])
          .describedAs(description)
          .isLessThanOrEqualTo(relativeBiasLimit[i]);
    }

    assertThat(relativeRmseLimit).describedAs(description).hasSize(relativeRmseValues.length);
    for (int i = 0; i < pValues.length; ++i) {
      assertThat(relativeRmseValues[i])
          .describedAs(description)
          .isLessThanOrEqualTo(relativeRmseLimit[i]);
    }
  }

  private double[] calculateErrorOfDistinctCountEqualOne(int p, ExaLogLog.Estimator estimator) {
    ExaLogLog sketch = create(p);
    double sumProbability = 0;
    double averageBias = 0;
    double averageRmse = 0;
    double trueDistinctCount = 1;

    PackedArray.PackedArrayHandler handler = getHandler(p);
    List<HashGenerator> hashGenerators = getHashGenerators(p);
    for (int genIdx1 = 0; genIdx1 < hashGenerators.size(); ++genIdx1) {
      HashGenerator hashGenerator1 = hashGenerators.get(genIdx1);
      double probProduct1 = hashGenerator1.getProbability();
      sketch.add(hashGenerator1.generateHashValue(0));
      sumProbability += probProduct1;

      double estimate = sketch.getDistinctCountEstimate(estimator);
      double error = estimate - trueDistinctCount;
      averageBias += probProduct1 * error;
      averageRmse += probProduct1 * (error * error);
      handler.set(sketch.getState(), 0, 0L);
    }

    double relativeBias = Math.abs(averageBias) / trueDistinctCount;
    double relativeRmse = Math.sqrt(averageRmse) / trueDistinctCount;

    assertThat(sumProbability).isCloseTo(1., Offset.offset(1e-6));
    assertThat(sketch.getState()).containsOnly((byte) 0);
    return new double[] {Math.abs(relativeBias), relativeRmse};
  }

  private double[] calculateErrorOfDistinctCountEqualTwo(int p, ExaLogLog.Estimator estimator) {

    long m = 1L << p;
    ExaLogLog sketch = create(p);
    double sumProbability = 0;
    double averageBias = 0;
    double averageRmse = 0;
    double trueDistinctCount = 2;

    PackedArray.PackedArrayHandler handler = getHandler(p);
    List<HashGenerator> hashGenerators = getHashGenerators(p);
    for (int genIdx1 = 0; genIdx1 < hashGenerators.size(); ++genIdx1) {
      HashGenerator hashGenerator1 = hashGenerators.get(genIdx1);
      double probProduct1 = hashGenerator1.getProbability() / m;
      for (int genIdx2 = genIdx1; genIdx2 < hashGenerators.size(); ++genIdx2) {
        HashGenerator hashGenerator2 = hashGenerators.get(genIdx2);
        double probProduct12 = probProduct1 * hashGenerator2.getProbability();
        {
          sketch.add(hashGenerator1.generateHashValue(0));
          sketch.add(hashGenerator2.generateHashValue(0));
          double probability = probProduct12;
          if (genIdx1 != genIdx2) probability *= 2;
          sumProbability += probability;
          double estimate = sketch.getDistinctCountEstimate(estimator);
          double error = estimate - trueDistinctCount;
          averageBias += probability * error;
          averageRmse += probability * (error * error);
          handler.set(sketch.getState(), 0, 0L);
        }
        if (m >= 2) {
          sketch.add(hashGenerator1.generateHashValue(0));
          sketch.add(hashGenerator2.generateHashValue(1));
          double probability = (m - 1) * probProduct12;
          if (genIdx1 != genIdx2) probability *= 2;
          sumProbability += probability;
          double estimate = sketch.getDistinctCountEstimate(estimator);
          double error = estimate - trueDistinctCount;
          averageBias += probability * error;
          averageRmse += probability * (error * error);
          handler.set(sketch.getState(), 0, 0L);
          handler.set(sketch.getState(), 1, 0L);
        }
      }
    }

    double relativeBias = Math.abs(averageBias) / trueDistinctCount;
    double relativeRmse = Math.sqrt(averageRmse) / trueDistinctCount;

    assertThat(sumProbability).isCloseTo(1., Offset.offset(1e-6));
    assertThat(sketch.getState()).containsOnly((byte) 0);

    return new double[] {relativeBias, relativeRmse};
  }

  private double[] calculateErrorOfDistinctCountEqualThree(int p, ExaLogLog.Estimator estimator) {
    long m = 1L << p;
    ExaLogLog sketch = create(p);
    double sumProbability = 0;
    double averageBias = 0;
    double averageRmse = 0;
    double trueDistinctCount = 3;

    PackedArray.PackedArrayHandler handler = getHandler(p);
    List<HashGenerator> hashGenerators = getHashGenerators(p);
    for (int genIdx1 = 0; genIdx1 < hashGenerators.size(); ++genIdx1) {
      HashGenerator hashGenerator1 = hashGenerators.get(genIdx1);
      double probProduct1 = hashGenerator1.getProbability() / m / m;
      for (int genIdx2 = genIdx1; genIdx2 < hashGenerators.size(); ++genIdx2) {
        HashGenerator hashGenerator2 = hashGenerators.get(genIdx2);
        double probProduct12 = probProduct1 * hashGenerator2.getProbability();
        for (int genIdx3 = 0; genIdx3 < hashGenerators.size(); ++genIdx3) {
          HashGenerator hashGenerator3 = hashGenerators.get(genIdx3);
          double probProduct123 = probProduct12 * hashGenerator3.getProbability();
          {
            sketch.add(hashGenerator1.generateHashValue(0));
            sketch.add(hashGenerator2.generateHashValue(0));
            sketch.add(hashGenerator3.generateHashValue(0));
            double probability = probProduct123;
            if (genIdx1 != genIdx2) probability *= 2;
            sumProbability += probability;
            double estimate = sketch.getDistinctCountEstimate(estimator);
            double error = estimate - trueDistinctCount;
            averageBias += probability * error;
            averageRmse += probability * (error * error);
            handler.set(sketch.getState(), 0, 0L);
          }
          if (m >= 2) {
            sketch.add(hashGenerator1.generateHashValue(0));
            sketch.add(hashGenerator2.generateHashValue(0));
            sketch.add(hashGenerator3.generateHashValue(1));
            double probability = (3 * (m - 1)) * probProduct123;
            if (genIdx1 != genIdx2) probability *= 2;
            sumProbability += probability;
            double estimate = sketch.getDistinctCountEstimate(estimator);
            double error = estimate - trueDistinctCount;
            averageBias += probability * error;
            averageRmse += probability * (error * error);
            handler.set(sketch.getState(), 0, 0L);
            handler.set(sketch.getState(), 1, 0L);
          }
          if (m >= 3) {
            sketch.add(hashGenerator1.generateHashValue(0));
            sketch.add(hashGenerator2.generateHashValue(1));
            sketch.add(hashGenerator3.generateHashValue(2));
            double probability = ((m - 1) * (m - 2)) * probProduct123;
            if (genIdx1 != genIdx2) probability *= 2;
            sumProbability += probability;
            double estimate = sketch.getDistinctCountEstimate(estimator);
            double error = estimate - trueDistinctCount;
            averageBias += probability * error;
            averageRmse += probability * (error * error);
            handler.set(sketch.getState(), 0, 0L);
            handler.set(sketch.getState(), 1, 0L);
            handler.set(sketch.getState(), 2, 0L);
          }
        }
      }
    }

    double relativeBias = Math.abs(averageBias) / trueDistinctCount;
    double relativeRmse = Math.sqrt(averageRmse) / trueDistinctCount;

    assertThat(sumProbability).isCloseTo(1., Offset.offset(1e-6));
    assertThat(sketch.getState()).containsOnly((byte) 0);

    return new double[] {relativeBias, relativeRmse};
  }

  protected ExaLogLog createFullSketch(int p) {
    ExaLogLog sketch = create(p);
    PackedArray.PackedArrayHandler handler = getHandler(p);
    for (HashGenerator hashGenerator : getHashGenerators(p)) {
      sketch.add(hashGenerator.generateHashValue(0));
    }
    byte[] state = sketch.getState();
    long x = handler.get(state, 0);
    for (int idx = 1; idx < 1 << p; ++idx) {
      handler.set(state, idx, x);
    }
    return sketch;
  }

  @Test
  void testDistinctCountEstimationFromFullSketch() {
    for (int p = getMinP(); p <= getMaxP(getT()); ++p) {
      ExaLogLog sketch = createFullSketch(p);
      assertThat(sketch.getDistinctCountEstimate()).isInfinite();
      for (ExaLogLog.Estimator estimator : getEstimators()) {
        assertThat(sketch.getDistinctCountEstimate(estimator)).isInfinite();
      }
    }
  }

  @Test
  void testChangeProbabilityAndFinitenessOfEstimators() {
    for (int p = getMinP(); p <= Math.min(12, getMaxP(getT())); ++p) {
      ExaLogLog sketch = create(p);
      MartingaleEstimator martingaleEstimator = new MartingaleEstimator();
      double stateChangeProbability = 1.;

      for (HashGenerator hashGenerator : getHashGenerators(p)) {
        assertThat(sketch.getStateChangeProbability()).isEqualTo(stateChangeProbability);
        assertThat(martingaleEstimator.getStateChangeProbability())
            .isEqualTo(stateChangeProbability);
        assertThat(sketch.getDistinctCountEstimate()).isFinite();
        for (ExaLogLog.Estimator estimator : getEstimators()) {
          assertThat(sketch.getDistinctCountEstimate(estimator)).isFinite();
        }
        for (int k = 0; k < (1 << p); ++k) {
          sketch.add(hashGenerator.generateHashValue(k), martingaleEstimator);
        }
        stateChangeProbability -= hashGenerator.getProbability();
      }
      assertThat(stateChangeProbability).isZero();
      assertThat(sketch.getStateChangeProbability()).isZero();
      assertThat(martingaleEstimator.getStateChangeProbability()).isZero();
      assertThat(sketch.getDistinctCountEstimate()).isInfinite();
      for (ExaLogLog.Estimator estimator : getEstimators()) {
        assertThat(sketch.getDistinctCountEstimate(estimator)).isInfinite();
      }
    }
  }

  @Test
  void testToken() {
    for (int p = getMinP(); p <= Math.min(getMaxP(getT()), 10); ++p) {
      ExaLogLog sketchToken = create(p);
      ExaLogLog sketchHash = create(p);
      ExaLogLog sketchTokenMartingale = create(p);
      ExaLogLog sketchHashMartingale = create(p);
      MartingaleEstimator tokenMartingaleEstimator = new MartingaleEstimator();
      MartingaleEstimator hashMartingaleEstimator = new MartingaleEstimator();

      List<HashGenerator> hashGenerators = getHashGenerators(p);

      for (HashGenerator hashGenerator : hashGenerators) {
        sketchToken.reset();
        sketchHash.reset();
        sketchTokenMartingale.reset();
        sketchHashMartingale.reset();
        tokenMartingaleEstimator.reset();
        hashMartingaleEstimator.reset();
        for (int k = 0; k < (1 << p); ++k) {

          long hash = hashGenerator.generateHashValue(k);
          int token = computeToken(hash);

          sketchToken.addToken(token);
          sketchHash.add(hash);
          sketchTokenMartingale.addToken(token, tokenMartingaleEstimator);
          sketchHashMartingale.add(hash, hashMartingaleEstimator);
        }
        assertThat(sketchToken.getState())
            .isEqualTo(sketchHash.getState())
            .isEqualTo(sketchTokenMartingale.getState())
            .isEqualTo(sketchHashMartingale.getState());
        assertThat(tokenMartingaleEstimator.getDistinctCountEstimate())
            .isEqualTo(hashMartingaleEstimator.getDistinctCountEstimate());
        assertThat(tokenMartingaleEstimator.getStateChangeProbability())
            .isEqualTo(hashMartingaleEstimator.getStateChangeProbability());
      }
    }
  }

  protected ExaLogLog merge(ExaLogLog sketch1, ExaLogLog sketch2) {
    return ExaLogLog.merge(sketch1, sketch2);
  }

  protected abstract int getT();

  protected abstract int getD();

  private static double hurvitzZeta(double x, double y) {
    double sum = 0;
    int u = 0;
    while (true) {
      double oldSum = sum;
      sum += Math.pow(u + y, -x);
      if (!(oldSum < sum)) return sum;
      u += 1;
    }
  }

  protected static double calculateTheoreticalRelativeStandardErrorConstantML(int t, int d) {
    double b = 2;
    for (int i = 0; i < t; ++i) {
      b = Math.sqrt(b);
    }
    return Math.sqrt(Math.log(b) / (hurvitzZeta(2., 1. + Math.pow(b, -d) / (b - 1.))));
  }

  protected static double calculateTheoreticalRelativeStandardErrorConstantMartingale(
      int t, int d) {
    double b = 2;
    for (int i = 0; i < t; ++i) {
      b = Math.sqrt(b);
    }
    return Math.sqrt(0.5 * Math.log(b) * (1. + Math.pow(b, -d) / (b - 1.)));
  }

  private static double calculateBiasCorrectionConstant(int t, int d) {
    double b = 2;
    for (int i = 0; i < t; ++i) {
      b = Math.sqrt(b);
    }
    double x = Math.pow(b, -d) / (b - 1.);

    return Math.log(b)
        * (1. + 2. * x)
        * hurvitzZeta(3., 1. + x)
        / Math.pow(hurvitzZeta(2., 1. + x), 2);
  }

  protected int getBitsPerRegister(int p) {
    return getD() + getT() + 6;
  }

  protected List<ExaLogLog.Estimator> getEstimators() {
    return Arrays.asList(MAXIMUM_LIKELIHOOD_ESTIMATOR);
  }

  protected int computeToken(long hashValue) {
    return ExaLogLog.computeToken(hashValue);
  }

  /** Returned hash-generators must be sorted in descending order of their probabilities. */
  protected List<HashGenerator> getHashGenerators(int p) {
    return TestUtils.getHashGenerators(p, getT());
  }

  @Test
  void testDistinctCountEqualOneMLEstimator() {
    testErrorOfDistinctCountEqualOne(
        new int[] {2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18},
        MAXIMUM_LIKELIHOOD_ESTIMATOR,
        p -> PrecomputedConstants.getTheoreticalRelativeErrorML(getT(), getD(), p),
        new double[] {
          0.0623, 0.0466, 0.0338, 0.0242, 0.0172, 0.0122, 0.0087, 0.0062, 0.0044, 0.0031, 0.0022,
          0.0016, 0.0011, 8.0E-4, 6.0E-4, 4.0E-4, 3.0E-4
        },
        new double[] {
          0.1691, 0.1106, 0.0757, 0.0527, 0.037, 0.0261, 0.0185, 0.0131, 0.0092, 0.0065, 0.0046,
          0.0033, 0.0023, 0.0017, 0.0012, 9.0E-4, 6.0E-4
        });
  }

  @Test
  void testDistinctCountEqualTwoMLEstimator() {
    testErrorOfDistinctCountEqualTwo(
        new int[] {2, 3, 4, 5, 6, 7, 8, 9, 10},
        MAXIMUM_LIKELIHOOD_ESTIMATOR,
        p -> PrecomputedConstants.getTheoreticalRelativeErrorML(getT(), getD(), p),
        new double[] {0.0614, 0.0463, 0.0337, 0.0242, 0.0172, 0.0122, 0.0087, 0.0062, 0.0044},
        new double[] {0.4974, 0.48, 0.4729, 0.4697, 0.4681, 0.4674, 0.467, 0.4668, 0.4667});
  }

  @Test
  void testDistinctCountEqualThreeMLEstimator() {
    testErrorOfDistinctCountEqualThree(
        new int[] {2, 3, 4, 5, 6, 7},
        MAXIMUM_LIKELIHOOD_ESTIMATOR,
        p -> PrecomputedConstants.getTheoreticalRelativeErrorML(getT(), getD(), p),
        new double[] {0.0606, 0.046, 0.0336, 0.0241, 0.0172, 0.0122},
        new double[] {0.5681, 0.5515, 0.5448, 0.5417, 0.5402, 0.5395});
  }

  @ParameterizedTest
  @MethodSource("someExaLogLogConfigurations")
  void testSomeBiasCorrectionConstants(int t, int d) {
    assertThat(ExaLogLog.ML_BIAS_CORRECTION_CONSTANTS[t][d])
        .isCloseTo(calculateBiasCorrectionConstant(t, d), withPercentage(1e-5));
  }

  @ParameterizedTest
  @MethodSource("someExaLogLogConfigurations")
  void testSomeTheoreticalRelativeErrorConstantsML(int t, int d) {
    assertThat(PrecomputedConstants.THEORETICAL_RELATIVE_ERROR_CONSTANTS_ML[t][d])
        .isCloseTo(calculateTheoreticalRelativeStandardErrorConstantML(t, d), withPercentage(1e-5));
  }

  @ParameterizedTest
  @MethodSource("someExaLogLogConfigurations")
  void testSomeTheoreticalRelativeErrorConstantsMartingale(int t, int d) {
    assertThat(PrecomputedConstants.THEORETICAL_RELATIVE_ERROR_CONSTANTS_MARTINGALE[t][d])
        .isCloseTo(
            calculateTheoreticalRelativeStandardErrorConstantMartingale(t, d),
            withPercentage(1e-5));
  }

  private static Stream<Arguments> someExaLogLogConfigurations() {
    return Stream.of(
        Arguments.of(0, 0),
        Arguments.of(0, 1),
        Arguments.of(0, 2),
        Arguments.of(1, 9),
        Arguments.of(2, 16),
        Arguments.of(2, 20),
        Arguments.of(2, 24));
  }

  @Test
  void testDistinctCountEstimation() {
    int maxP = 12;
    long[] distinctCounts = TestUtils.getDistinctCountValues(0, 100000, 0.2);
    SplittableRandom random = new SplittableRandom(0xffc4b333c0c89271L);
    for (int p = getMinP(); p <= maxP; ++p) {
      testDistinctCountEstimation(
          p,
          random.nextLong(),
          distinctCounts,
          Arrays.asList(MAXIMUM_LIKELIHOOD_ESTIMATOR),
          Arrays.asList(
              pp -> PrecomputedConstants.getTheoreticalRelativeErrorML(getT(), getD(), pp)),
          new double[] {0.12},
          new double[] {1.39},
          new double[] {0.31},
          0.1,
          1.56,
          0.28,
          MAXIMUM_LIKELIHOOD_ESTIMATOR);
    }
  }

  @Test
  void testLargeDistinctCountEstimation() {
    testLargeDistinctCountEstimation(
        8,
        0x746bb99d86c31b4dL,
        1_000_000_000L,
        Arrays.asList(MAXIMUM_LIKELIHOOD_ESTIMATOR),
        Arrays.asList(p -> PrecomputedConstants.getTheoreticalRelativeErrorML(getT(), getD(), p)),
        0.04,
        0.025);
  }
}
