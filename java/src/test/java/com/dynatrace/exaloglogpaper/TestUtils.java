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

import static com.dynatrace.hash4j.util.Preconditions.checkArgument;
import static java.util.Comparator.comparing;

import com.dynatrace.hash4j.random.PseudoRandomGenerator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

public final class TestUtils {

  private TestUtils() {}

  /*  public static long[] getDistinctCountValues(long min, long max, double relativeIncrement) {
    List<Long> distinctCounts = new ArrayList<>();
    final double factor = 1. / (1. + relativeIncrement);
    for (long c = max; c >= min; c = Math.min(c - 1, (long) Math.ceil(c * factor))) {
      distinctCounts.add(c);
    }
    Collections.reverse(distinctCounts);
    return distinctCounts.stream().mapToLong(Long::valueOf).toArray();
  }*/

  public static List<BigInt> getDistinctCountValues(double max, double relativeIncrement) {
    checkArgument(max >= 1.);
    List<BigInt> distinctCounts = new ArrayList<>();
    BigInt c = BigInt.ceil(max);
    final double factor = 1. / (1. + relativeIncrement);
    while (c.isPositive()) {
      distinctCounts.add(c.copy());
      double d = c.asDouble();
      c.decrement();
      c.min(BigInt.ceil(d * factor));
    }
    Collections.reverse(distinctCounts);
    return distinctCounts;
  }

  public static long[] getDistinctCountValues(long min, long max, double relativeIncrement) {
    List<Long> distinctCounts = new ArrayList<>();
    final double factor = 1. / (1. + relativeIncrement);
    for (long c = max; c >= min; c = Math.min(c - 1, (long) Math.ceil(c * factor))) {
      distinctCounts.add(c);
    }
    Collections.reverse(distinctCounts);
    return distinctCounts.stream().mapToLong(Long::valueOf).toArray();
  }

  public interface HashGenerator {

    double getProbability();

    long generateHashValue(int registerIndex);
  }

  public static List<HashGenerator> getHashGenerators(int p, int t) {
    List<HashGenerator> generators = new ArrayList<>();

    for (int updateValue = 1; updateValue <= ((65 - p - t) << t); ++updateValue) {

      final double probability =
          Double.longBitsToDouble(
              (0x3ffL - Math.min(t + 1 + ((updateValue - 1) >>> t), 64 - p)) << 52);

      int sub = (updateValue - 1) & ((1 << t) - 1);
      int nlz = (updateValue - 1) >>> t;

      final long z = (0xFFFFFFFFFFFFFFFFL >>> nlz >>> (t + p) << (t + p)) | sub;

      generators.add(
          new HashGenerator() {
            @Override
            public double getProbability() {
              return probability;
            }

            @Override
            public long generateHashValue(int registerIndex) {
              return z | (registerIndex << t);
            }
          });
    }
    return generators;
  }

  public static final class Transition {
    private final BigInt distinctCount;
    private final long hash;

    public BigInt getDistinctCount() {
      return distinctCount;
    }

    public long getHash() {
      return hash;
    }

    public Transition(BigInt distinctCount, long hash) {
      this.distinctCount = distinctCount;
      this.hash = hash;
    }
  }

  public static void generateTransitions(
      Transition[] transitions,
      BigInt distinctCountOffset,
      List<HashGenerator> hashGenerators,
      int p,
      PseudoRandomGenerator prg) {

    int counter = 0;
    for (HashGenerator hashGenerator : hashGenerators) {
      double factor = (1 << p) / hashGenerator.getProbability();
      for (int idx = 0; idx < (1 << p); ++idx) {
        BigInt transitionDistinctCount = BigInt.floor(prg.nextExponential() * factor);
        transitionDistinctCount.increment(); // 1-based geometric distribution
        transitionDistinctCount.add(distinctCountOffset);
        long hash = hashGenerator.generateHashValue(idx);
        transitions[counter++] = new Transition(transitionDistinctCount, hash);
      }
    }
    Arrays.sort(transitions, comparing(Transition::getDistinctCount));
  }

  static double calculateBias(double[] empiricalValues, double trueValue) {
    return DoubleStream.of(empiricalValues).map(x -> x - trueValue).average().getAsDouble()
        / trueValue;
  }

  static double calculateRmse(double[] empiricalValues, double trueValue) {
    return Math.sqrt(
            DoubleStream.of(empiricalValues)
                .map(x -> (x - trueValue) * (x - trueValue))
                .average()
                .getAsDouble())
        / trueValue;
  }

  static boolean generateBernoulli(double probability, PseudoRandomGenerator prg) {
    if (probability == 1) return true;
    if (probability == 0) return false;
    while (true) {
      probability *= 0x1p63;
      long probabilityLow = (long) Math.floor(probability);
      long probabilityHigh = (long) Math.ceil(probability);
      long random = prg.nextLong() & 0x7FFFFFFFFFFFFFFFL;
      if (random < probabilityLow) return true;
      if (random >= probabilityHigh) return false;
      probability -= probabilityLow;
      long probabilityDiff = probabilityHigh - probabilityLow;
      if (probabilityDiff > 1) probability /= probabilityDiff;
    }
  }

  private static final double POISSON_GENERATION_THRESHOLD = 0.05; // should be smaller than 1

  static ExaLogLog generateExaLogLog(
      double distinctCount, int t, int d, int p, PseudoRandomGenerator prg) {
    long distinctCountLong = (long) distinctCount;
    double relativeErrorForPoisson = 1. / Math.sqrt(distinctCount);
    if (distinctCountLong != distinctCount
        || relativeErrorForPoisson
            < POISSON_GENERATION_THRESHOLD
                * PrecomputedConstants.getTheoreticalRelativeErrorML(t, d, p)) {
      return generateExaLogLogPoisson(distinctCount, t, d, p, prg);
    } else {
      return generateExaLogLogExact(distinctCountLong, t, d, p, prg);
    }
  }

  private static ExaLogLog generateExaLogLogExact(
      long distinctCount, int t, int d, int p, PseudoRandomGenerator prg) {
    checkArgument(distinctCount >= 0);
    ExaLogLog sketch = ExaLogLog.create(t, d, p);
    for (long i = 0; i < distinctCount; ++i) {
      sketch.add(prg.nextLong());
    }
    return sketch;
  }

  static final double[] POW_0_5 =
      IntStream.range(0, 65).mapToDouble(i -> Math.pow(0.5, i)).toArray();

  static int phi(long k, int p, int t) {
    return (int) Math.min((t + 1 + ((k - 1) >>> t)), 64 - p);
  }

  private static final long generateHashValue(int updateValue, int registerIndex, int p, int t) {
    int sub = (updateValue - 1) & ((1 << t) - 1);
    int nlz = (updateValue - 1) >>> t;

    final long z = (0xFFFFFFFFFFFFFFFFL >>> nlz >>> (t + p) << (t + p)) | sub;

    return z | (registerIndex << t);
  }

  private static ExaLogLog generateExaLogLogPoisson(
      double distinctCount, int t, int d, int p, PseudoRandomGenerator prg) {
    checkArgument(distinctCount >= 0);
    ExaLogLog sketch = ExaLogLog.create(t, d, p);
    int m = 1 << p;
    int maxUpdateValue = (65 - p - t) << t;

    double[] probabilities = new double[64 - t - p];
    for (int ph = t + 1; ph <= 64 - p; ph += 1) {
      probabilities[ph - t - 1] = -Math.expm1(-distinctCount / m * POW_0_5[ph]);
    }

    for (int i = 0; i < m; ++i) {
      int max = 0;
      for (int k = maxUpdateValue; k >= 1 && k >= max - d; --k) {
        if (generateBernoulli(probabilities[phi(k, p, t) - t - 1], prg)) {
          sketch.add(generateHashValue(k, i, p, t));
          if (max == 0) max = k;
        }
      }
    }
    return sketch;
  }

  public static double hurvitzZeta(double x, double y) {
    double sum = 0;
    int u = 0;
    while (true) {
      double oldSum = sum;
      sum += Math.pow(u + y, -x);
      if (!(oldSum < sum)) return sum;
      u += 1;
    }
  }
}
