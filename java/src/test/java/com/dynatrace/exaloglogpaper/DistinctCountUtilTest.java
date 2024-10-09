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

import static com.dynatrace.exaloglogpaper.DistinctCountUtil.*;
import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.data.Percentage.withPercentage;

import com.dynatrace.hash4j.random.PseudoRandomGenerator;
import com.dynatrace.hash4j.random.PseudoRandomGeneratorProvider;
import java.util.Arrays;
import java.util.SplittableRandom;
import java.util.concurrent.atomic.AtomicLong;
import org.assertj.core.data.Percentage;
import org.hipparchus.analysis.UnivariateFunction;
import org.hipparchus.analysis.solvers.BisectionSolver;
import org.junit.jupiter.api.Test;

class DistinctCountUtilTest {

  private static double solve1(double a, int b0) {
    return Math.log1p(b0 / a);
  }

  private static double solve2(double a, int b0, int b1) {
    return 2.
        * Math.log(
            (0.5 * b1 + Math.sqrt(Math.pow(0.5 * b1, 2) + 4. * a * (a + b0 + 0.5 * b1)))
                / (2. * a));
  }

  private static double solveN(double a, int... b) {
    BisectionSolver bisectionSolver = new BisectionSolver(1e-12, 1e-12);
    UnivariateFunction function =
        x -> {
          double sum = 0;
          if (b != null) {
            for (int i = 0; i < b.length; ++i) {
              if (b[i] > 0) {
                double f = Double.longBitsToDouble((0x3ffL - i) << 52);
                sum += b[i] * f / Math.expm1(x * f);
              }
            }
          }
          sum -= a;
          return sum;
        };
    return bisectionSolver.solve(Integer.MAX_VALUE, function, 0, Double.MAX_VALUE);
  }

  @FunctionalInterface
  private interface Solver {
    double solve(double a, int[] b, int n);
  }

  private void testMaximumLikelihoodEquationSolver(Solver solver, double maxRelativeError) {

    double maxRelativeErrorPercent = maxRelativeError * 100;

    assertThat(solver.solve(1., new int[] {}, -1)).isZero();
    assertThat(solver.solve(2., new int[] {}, -1)).isZero();
    assertThat(solver.solve(0., new int[] {1}, 0)).isPositive().isInfinite();
    assertThat(solver.solve(0., new int[] {1, 0}, 1)).isPositive().isInfinite();

    assertThat(solver.solve(1., new int[] {1}, 0))
        .isCloseTo(solve1(1., 1), withPercentage(maxRelativeErrorPercent));
    assertThat(solver.solve(2., new int[] {3}, 0))
        .isCloseTo(solve1(2., 3), withPercentage(maxRelativeErrorPercent));
    assertThat(solver.solve(3., new int[] {2}, 0))
        .isCloseTo(solve1(3., 2), withPercentage(maxRelativeErrorPercent));
    assertThat(solver.solve(5., new int[] {7}, 0))
        .isCloseTo(solve1(5., 7), withPercentage(maxRelativeErrorPercent));
    assertThat(solver.solve(11., new int[] {7}, 0))
        .isCloseTo(solve1(11., 7), withPercentage(maxRelativeErrorPercent));
    assertThat(solver.solve(0.03344574927673416, new int[] {238}, 0))
        .isCloseTo(solve1(0.03344574927673416, 238), withPercentage(maxRelativeErrorPercent));

    assertThat(solver.solve(3., new int[] {2, 0}, 1))
        .isCloseTo(solve2(3., 2, 0), withPercentage(maxRelativeErrorPercent));
    assertThat(solver.solve(5., new int[] {7, 0}, 1))
        .isCloseTo(solve2(5., 7, 0), withPercentage(maxRelativeErrorPercent));
    assertThat(solver.solve(11., new int[] {7, 0}, 1))
        .isCloseTo(solve2(11., 7, 0), withPercentage(maxRelativeErrorPercent));
    assertThat(solver.solve(0.12274207925281233, new int[] {574, 580}, 1))
        .isCloseTo(solve2(0.12274207925281233, 574, 580), withPercentage(maxRelativeErrorPercent));

    assertThat(solver.solve(1., new int[] {2, 3}, 1))
        .isCloseTo(solve2(1., 2, 3), withPercentage(maxRelativeErrorPercent));
    assertThat(solver.solve(3., new int[] {2, 1}, 1))
        .isCloseTo(solve2(3., 2, 1), withPercentage(maxRelativeErrorPercent));

    assertThat(solver.solve(3., new int[] {2, 1, 4, 5}, 3))
        .isCloseTo(solveN(3., 2, 1, 4, 5), withPercentage(maxRelativeErrorPercent));
    assertThat(solver.solve(3., new int[] {6, 7, 2, 1, 4, 5}, 5))
        .isCloseTo(solveN(3., 6, 7, 2, 1, 4, 5), withPercentage(maxRelativeErrorPercent));
    assertThat(solver.solve(7., new int[] {0, 0, 6, 7, 2, 1, 4, 5, 0, 0, 0, 0}, 11))
        .isCloseTo(
            solveN(7., 0, 0, 6, 7, 2, 1, 4, 5, 0, 0, 0, 0),
            withPercentage(maxRelativeErrorPercent));
    assertThat(solver.solve(7., new int[] {0, 0, 6, 7, 0, 0, 4, 5, 0, 0, 0, 0}, 11))
        .isCloseTo(
            solveN(7., 0, 0, 6, 7, 0, 0, 4, 5, 0, 0, 0, 0),
            withPercentage(maxRelativeErrorPercent));
    assertThat(solver.solve(0x1p-64, new int[] {0}, -1))
        .isCloseTo(solve1(0x1p-64, 0), withPercentage(maxRelativeErrorPercent));
    assertThat(solver.solve(0x1p-64, new int[] {1}, 0))
        .isCloseTo(solve1(0x1p-64, 1), withPercentage(maxRelativeErrorPercent));
    {
      int[] b = new int[64];
      b[63] = 1;
      assertThat(solver.solve(1., b, 63))
          .isCloseTo(solveN(1., b), withPercentage(maxRelativeErrorPercent));
    }
    {
      double a = 0.1991679290502678;
      int[] b = {1, 1, 1};
      int n = 2;
      assertThat(solver.solve(a, b, n))
          .isCloseTo(solveN(a, b), withPercentage(maxRelativeErrorPercent));
    }
    {
      double a = 0.20711847986587495;
      int[] b = {1, 1, 1};
      int n = 2;
      assertThat(solver.solve(a, b, n))
          .isCloseTo(solveN(a, b), withPercentage(maxRelativeErrorPercent));
    }

    // many more cases to have full code coverage
    SplittableRandom random = new SplittableRandom(0x93b723ca5f234685L);
    for (int i = 0; i < 10000; ++i) {
      double a = 1. - random.nextDouble();
      int b0 = random.nextInt(1000);
      assertThat(solver.solve(a, new int[] {b0}, 0))
          .isCloseTo(solve1(a, b0), withPercentage(maxRelativeErrorPercent));
    }

    {
      int[] b = new int[64];
      b[0] = 1;
      b[63] = 1;
      assertThat(solver.solve(1., b, 63))
          .isCloseTo(1.4455749111515481, withPercentage(maxRelativeErrorPercent));
    }
  }

  @Test
  void testSolveMaximumLikelihoodEquation() {
    testMaximumLikelihoodEquationSolver(
        (a, b, n) -> solveMaximumLikelihoodEquation(a, b, n, 1e-2), 5e-6);
    testMaximumLikelihoodEquationSolver(
        (a, b, n) -> solveMaximumLikelihoodEquation(a, b, n, 1e-4), 2e-10);
    testMaximumLikelihoodEquationSolver(
        (a, b, n) -> solveMaximumLikelihoodEquation(a, b, n, 1e-6), 5e-13);
    testMaximumLikelihoodEquationSolver(
        (a, b, n) -> solveMaximumLikelihoodEquation(a, b, n, 1e-8), 5e-13);
    testMaximumLikelihoodEquationSolver(
        (a, b, n) -> solveMaximumLikelihoodEquation(a, b, n, 1e-10), 5e-13);
    testMaximumLikelihoodEquationSolver(
        (a, b, n) -> solveMaximumLikelihoodEquation(a, b, n, 1e-12), 5e-13);
    testMaximumLikelihoodEquationSolver(
        (a, b, n) -> solveMaximumLikelihoodEquation(a, b, n, 1e-14), 5e-13);
    testMaximumLikelihoodEquationSolver(
        (a, b, n) -> solveMaximumLikelihoodEquation(a, b, n, 0.), 5e-13);
    testMaximumLikelihoodEquationSolver(
        (a, b, n) -> solveMaximumLikelihoodEquation(a, b, n, Double.NEGATIVE_INFINITY), 2e-12);
    testMaximumLikelihoodEquationSolver(
        (a, b, n) -> solveMaximumLikelihoodEquation(a, b, n, Double.NaN), 2e-12);
  }

  @Test
  void testSolveMaximumLikelihoodEquationNumIterations() {
    AtomicLong iterationCounter = new AtomicLong();
    solveMaximumLikelihoodEquation(3.5, new int[] {1, 2, 3, 4, 5, 6}, 5, 0., iterationCounter);
    assertThat(iterationCounter.get()).isEqualTo(4);
  }

  private static TokenIterable fromSortedArray(int[] tokens) {
    return () ->
        new TokenIterator() {
          private int idx = 0;

          @Override
          public boolean hasNext() {
            return idx < tokens.length;
          }

          @Override
          public int nextToken() {
            return tokens[idx++];
          }
        };
  }

  private static void testEstimationFromTokens(
      int distinctCount, int tokenParameter, double maxErrorInPercent) {

    PseudoRandomGenerator prg = PseudoRandomGeneratorProvider.splitMix64_V1().create();
    prg.reset(0L);

    int numIterations = 10;
    int[] tokens = new int[distinctCount];

    for (int i = 0; i < numIterations; ++i) {
      for (int c = 0; c < distinctCount; ++c) {
        tokens[c] = DistinctCountUtil.computeToken(prg.nextLong(), tokenParameter);
      }
      Arrays.sort(tokens);

      double estimate =
          DistinctCountUtil.estimateDistinctCountFromTokens(
              fromSortedArray(tokens), tokenParameter);
      assertThat(estimate).isCloseTo(distinctCount, Percentage.withPercentage(maxErrorInPercent));
    }
  }

  @Test
  void testEstimationFromTokens() {
    int[] tokenParameters = {10, 12, 15, 18, 22, 26};
    double[] maxErrorInPercentValues = {4, 2.5, 1, 1, 1, 1};
    int[] distinctCounts = {1, 2, 3, 5, 10, 100, 1000, 10000, 100000, 1000000, 10000000};
    for (int i = 0; i < tokenParameters.length; ++i) {
      int tokenParameter = tokenParameters[i];
      double maxErrorInPercent = maxErrorInPercentValues[i];
      for (int distinctCount : distinctCounts) {
        testEstimationFromTokens(distinctCount, tokenParameter, maxErrorInPercent);
      }
    }
  }

  private static TokenIterable getTestTokens(int maxTokenExclusive) {

    long maxTokenExclusiveLong = maxTokenExclusive & 0xFFFFFFFFL;
    return () ->
        new TokenIterator() {
          private long state = 0;

          @Override
          public boolean hasNext() {
            return state < maxTokenExclusiveLong;
          }

          @Override
          public int nextToken() {
            return (int) state++;
          }
        };
  }

  @Test
  void testEstimationFromZeroTokens() {
    for (int tokenParameter = TOKEN_PARAMETER_MIN;
        tokenParameter <= TOKEN_PARAMETER_MAX;
        ++tokenParameter) {
      double estimate =
          DistinctCountUtil.estimateDistinctCountFromTokens(getTestTokens(0), tokenParameter);
      assertThat(estimate).isZero();
    }
  }

  private static int getMaxValidToken(int tokenParameter) {
    return (int) (((0xFFFFFFFFFFFFFFFFL >>> -tokenParameter) << 6) + 64 - tokenParameter);
  }

  @Test
  void testEstimationFromAllTokens() {
    for (int tokenParameter = TOKEN_PARAMETER_MIN;
        tokenParameter <= TOKEN_PARAMETER_MAX;
        ++tokenParameter) {
      double estimate =
          DistinctCountUtil.estimateDistinctCountFromTokens(
              getTestTokens(getMaxValidToken(tokenParameter) + 1), tokenParameter);
      assertThat(estimate).isInfinite();
    }
  }

  @Test
  void testEstimationFromAlmostAllTokens() {
    for (int tokenParameter = TOKEN_PARAMETER_MIN;
        tokenParameter <= TOKEN_PARAMETER_MAX;
        ++tokenParameter) {
      double estimate =
          DistinctCountUtil.estimateDistinctCountFromTokens(
              getTestTokens(getMaxValidToken(tokenParameter)), tokenParameter);
      assertThat(estimate).isFinite().isGreaterThan(1e19);
    }
  }

  @Test
  void testComputeToken2() {

    SplittableRandom random = new SplittableRandom(0x026680003f978228L);

    int numCycles = 100;

    for (int tokenParameter = TOKEN_PARAMETER_MIN;
        tokenParameter <= TOKEN_PARAMETER_MAX;
        ++tokenParameter) {

      for (int nlz = 0; nlz <= 64 - tokenParameter; ++nlz) {

        for (int i = 0; i < numCycles; ++i) {
          long hash = random.nextLong();
          int token = DistinctCountUtil.computeToken(hash, tokenParameter);
          long reconstructedHash = DistinctCountUtil.reconstructHash(token, tokenParameter);
          int tokenFromReconstructedHash =
              DistinctCountUtil.computeToken(reconstructedHash, tokenParameter);
          assertThat(reconstructedHash)
              .isEqualTo(
                  hash | (((0xFFFFFFFFFFFFFFFFL >>> tokenParameter >>> token) << tokenParameter)));
          assertThat(tokenFromReconstructedHash).isEqualTo(token);
        }
      }
    }
  }

  @Test
  void testUnsignedLongToDouble() {
    assertThat(DistinctCountUtil.unsignedLongToDouble(0)).isZero();
    assertThat(DistinctCountUtil.unsignedLongToDouble(1)).isOne();
    assertThat(DistinctCountUtil.unsignedLongToDouble(0x8000000000000000L)).isEqualTo(0x1p63);
  }

  @Test
  void testIsValidToken() {
    assertThat(isValidToken(0, 7)).isTrue();
    assertThat(isValidToken(10000, 1)).isFalse();
    assertThat(isValidToken(0xFFFFFFFF, 1)).isFalse();
    assertThat(isValidToken(0x3F, 2)).isFalse();
  }
}
