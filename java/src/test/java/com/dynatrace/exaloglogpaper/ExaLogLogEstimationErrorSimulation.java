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

import com.dynatrace.hash4j.random.PseudoRandomGenerator;
import com.dynatrace.hash4j.random.PseudoRandomGeneratorProvider;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

public class ExaLogLogEstimationErrorSimulation {
  public static void main(String[] args) {
    int t = Integer.parseInt(args[0]);
    int d = Integer.parseInt(args[1]);
    int p = Integer.parseInt(args[2]);
    String outputFile = args[3];
    doSimulation(t, d, p, "exaloglog", outputFile, TestUtils.getHashGenerators(p, t));
  }

  private static final class LocalState {
    private final ExaLogLog sketch;
    private final TestUtils.Transition[] transitions;
    private final PseudoRandomGenerator prg;
    private final List<TestUtils.HashGenerator> hashGenerators;
    private final int p;

    public LocalState(
        ExaLogLog sketch,
        PseudoRandomGenerator prg,
        List<TestUtils.HashGenerator> hashGenerators,
        int p) {
      this.sketch = sketch;
      this.transitions = new TestUtils.Transition[hashGenerators.size() * (1 << p)];
      this.prg = prg;
      this.hashGenerators = hashGenerators;
      this.p = p;
    }

    public void generateTransitions(BigInt distinctCountOffset) {
      TestUtils.generateTransitions(transitions, distinctCountOffset, hashGenerators, p, prg);
    }
  }

  private static final ExaLogLog.MaximumLikelihoodEstimator MAXIMUM_LIKELIHOOD_ESTIMATOR =
      new ExaLogLog.MaximumLikelihoodEstimator();

  public static void doSimulation(
      int t,
      int d,
      int p,
      String sketchName,
      String outputFile,
      List<TestUtils.HashGenerator> hashGenerators) {

    // parameters
    int numCycles = 100_000;
    int maxParallelism = 16;
    final BigInt largeScaleSimulationModeDistinctCountLimit = BigInt.fromLong(1000000);
    List<BigInt> targetDistinctCounts = TestUtils.getDistinctCountValues(1e21, 0.05);

    SplittableRandom seedRandom = new SplittableRandom(0x891ea7f506edfc35L);
    long[] seeds = seedRandom.longs(numCycles).toArray();

    long[][] numIterations = new long[targetDistinctCounts.size()][];
    double[][] estimatedDistinctCountsML = new double[targetDistinctCounts.size()][];
    double[][] estimatedDistinctCountsMartingale = new double[targetDistinctCounts.size()][];
    for (int i = 0; i < targetDistinctCounts.size(); ++i) {
      numIterations[i] = new long[numCycles];
      estimatedDistinctCountsML[i] = new double[numCycles];
      estimatedDistinctCountsMartingale[i] = new double[numCycles];
    }

    PseudoRandomGeneratorProvider prgProvider = PseudoRandomGeneratorProvider.splitMix64_V1();
    ThreadLocal<LocalState> localStates =
        ThreadLocal.withInitial(
            () ->
                new LocalState(ExaLogLog.create(t, d, p), prgProvider.create(), hashGenerators, p));

    try {
      ForkJoinPool forkJoinPool =
          new ForkJoinPool(Math.min(ForkJoinPool.getCommonPoolParallelism(), maxParallelism));
      forkJoinPool
          .submit(
              () ->
                  IntStream.range(0, numCycles)
                      .parallel()
                      .forEach(
                          i -> {
                            LocalState state = localStates.get();
                            final PseudoRandomGenerator prg = state.prg;
                            prg.reset(seeds[i]);
                            final ExaLogLog sketch = state.sketch;
                            sketch.reset();
                            MartingaleEstimator martingaleEstimator = new MartingaleEstimator();
                            final TestUtils.Transition[] transitions = state.transitions;
                            state.generateTransitions(largeScaleSimulationModeDistinctCountLimit);

                            BigInt trueDistinctCount = BigInt.createZero();
                            int transitionIndex = 0;
                            for (int distinctCountIndex = 0;
                                distinctCountIndex < targetDistinctCounts.size();
                                ++distinctCountIndex) {
                              BigInt targetDistinctCount =
                                  targetDistinctCounts.get(distinctCountIndex);
                              BigInt limit = targetDistinctCount.copy();
                              limit.min(largeScaleSimulationModeDistinctCountLimit);

                              while (trueDistinctCount.compareTo(limit) < 0) {
                                sketch.add(prg.nextLong(), martingaleEstimator);
                                trueDistinctCount.increment();
                              }
                              if (trueDistinctCount.compareTo(targetDistinctCount) < 0) {
                                while (transitionIndex < transitions.length
                                    && transitions[transitionIndex]
                                            .getDistinctCount()
                                            .compareTo(targetDistinctCount)
                                        <= 0) {
                                  sketch.add(
                                      transitions[transitionIndex].getHash(), martingaleEstimator);
                                  transitionIndex += 1;
                                }
                                trueDistinctCount.set(targetDistinctCount);
                              }

                              AtomicLong iterationCounter = new AtomicLong();
                              estimatedDistinctCountsML[distinctCountIndex][i] =
                                  MAXIMUM_LIKELIHOOD_ESTIMATOR.estimate(sketch, iterationCounter);
                              numIterations[distinctCountIndex][i] = iterationCounter.get();
                              estimatedDistinctCountsMartingale[distinctCountIndex][i] =
                                  martingaleEstimator.getDistinctCountEstimate();
                            }
                          }))
          .get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }

    double theoreticalRelativeStandardErrorsML =
        PrecomputedConstants.getTheoreticalRelativeErrorML(t, d, p);
    double theoreticalRelativeStandardErrorsMartingale =
        PrecomputedConstants.getTheoreticalRelativeErrorMartingale(t, d, p);

    try (FileWriter writer = new FileWriter(outputFile, StandardCharsets.UTF_8)) {
      writer.write(
          "sketch_name="
              + sketchName
              + "; t="
              + t
              + "; d="
              + d
              + "; p="
              + p
              + "; num_cycles="
              + numCycles
              + "; large_scale_simulation_mode_distinct_count_limit="
              + largeScaleSimulationModeDistinctCountLimit
              + "\n");
      writer.write("distinct count");
      writer.write("; relative bias maximum likelihood");
      writer.write("; relative rmse maximum likelihood");
      writer.write("; theoretical relative standard error maximum likelihood");
      writer.write("; relative bias martingale");
      writer.write("; relative rmse martingale");
      writer.write("; theoretical relative standard error martingale");
      writer.write("; min num iterations");
      writer.write("; mean num iterations");
      writer.write("; max num iterations");
      writer.write('\n');

      for (int distinctCountIndex = 0;
          distinctCountIndex < targetDistinctCounts.size();
          ++distinctCountIndex) {

        double trueDistinctCount = targetDistinctCounts.get(distinctCountIndex).asDouble();
        double relativeBiasML =
            TestUtils.calculateBias(
                estimatedDistinctCountsML[distinctCountIndex], trueDistinctCount);
        double relativeRmseML =
            TestUtils.calculateRmse(
                estimatedDistinctCountsML[distinctCountIndex], trueDistinctCount);
        double relativeBiasMartingale =
            TestUtils.calculateBias(
                estimatedDistinctCountsMartingale[distinctCountIndex], trueDistinctCount);
        double relativeRmseMartingale =
            TestUtils.calculateRmse(
                estimatedDistinctCountsMartingale[distinctCountIndex], trueDistinctCount);
        long minNumIterations = LongStream.of(numIterations[distinctCountIndex]).min().getAsLong();
        double meanNumIterations =
            LongStream.of(numIterations[distinctCountIndex]).average().getAsDouble();
        long maxNumIterations = LongStream.of(numIterations[distinctCountIndex]).max().getAsLong();

        writer.write("" + trueDistinctCount);
        writer.write("; " + relativeBiasML);
        writer.write("; " + relativeRmseML);
        writer.write("; " + theoreticalRelativeStandardErrorsML);
        writer.write("; " + relativeBiasMartingale);
        writer.write("; " + relativeRmseMartingale);
        writer.write("; " + theoreticalRelativeStandardErrorsMartingale);
        writer.write("; " + minNumIterations);
        writer.write("; " + meanNumIterations);
        writer.write("; " + maxNumIterations);

        writer.write('\n');
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
