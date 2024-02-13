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

import static com.dynatrace.hash4j.util.Preconditions.checkArgument;
import static java.util.Comparator.comparing;

import com.dynatrace.hash4j.random.PseudoRandomGenerator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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

  // used for HyperLogLog and UltraLogLog
  public static List<HashGenerator> getHashGenerators1(int p) {
    List<HashGenerator> generators = new ArrayList<>();

    for (int updateValue = 1; updateValue <= 65 - p; ++updateValue) {

      final double probability =
          Double.longBitsToDouble((0x3ffL - Math.min(updateValue, 64 - p)) << 52);
      int nlz = updateValue - 1;
      final long z = (nlz < 64) ? 0xFFFFFFFFFFFFFFFFL >>> p >>> nlz : 0L;

      generators.add(
          new HashGenerator() {
            @Override
            public double getProbability() {
              return probability;
            }

            @Override
            public long generateHashValue(int registerIndex) {
              return z | (((long) registerIndex) << -p);
            }
          });
    }
    return generators;
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
}
