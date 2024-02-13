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

import com.dynatrace.exaloglogpaper.TestUtils.HashGenerator;
import com.dynatrace.exaloglogpaper.TestUtils.Transition;
import com.dynatrace.hash4j.random.PseudoRandomGenerator;
import com.dynatrace.hash4j.random.PseudoRandomGeneratorProvider;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.function.IntToDoubleFunction;
import java.util.function.Supplier;
import java.util.function.ToDoubleBiFunction;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

public final class EstimationErrorSimulationUtil {

  private EstimationErrorSimulationUtil() {}

  public static final class EstimatorConfig {
    private final ToDoubleBiFunction<ExaLogLog, MartingaleEstimator> estimator;
    private final String label;

    private final IntToDoubleFunction pToAsymptoticRelativeStandardError;

    public EstimatorConfig(
        ToDoubleBiFunction<ExaLogLog, MartingaleEstimator> estimator,
        String label,
        IntToDoubleFunction pToAsymptoticRelativeStandardError) {
      this.estimator = estimator;
      this.label = label;
      this.pToAsymptoticRelativeStandardError = pToAsymptoticRelativeStandardError;
    }

    public String getLabel() {
      return label;
    }

    public IntToDoubleFunction getpToAsymptoticRelativeStandardError() {
      return pToAsymptoticRelativeStandardError;
    }
  }

  private static final class LocalState {
    private final ExaLogLog sketch;
    private final Transition[] transitions;
    private final PseudoRandomGenerator prg;
    private final List<HashGenerator> hashGenerators;
    private final int p;

    public LocalState(
        ExaLogLog sketch, PseudoRandomGenerator prg, List<HashGenerator> hashGenerators, int p) {
      this.sketch = sketch;
      this.transitions = new Transition[hashGenerators.size() * (1 << p)];
      this.prg = prg;
      this.hashGenerators = hashGenerators;
      this.p = p;
    }

    public void generateTransitions(BigInt distinctCountOffset) {
      TestUtils.generateTransitions(transitions, distinctCountOffset, hashGenerators, p, prg);
    }
  }

  public static void doSimulation(
      int t,
      int d,
      int p,
      String sketchName,
      Supplier<ExaLogLog> supplier,
      List<EstimatorConfig> estimatorConfigs,
      String outputFile,
      List<HashGenerator> hashGenerators) {

    // parameters
    int numCycles = 100000;
    int maxParallelism = 16;
    final BigInt largeScaleSimulationModeDistinctCountLimit = BigInt.fromLong(1000000);
    List<BigInt> targetDistinctCounts = TestUtils.getDistinctCountValues(1e21, 0.05);

    SplittableRandom seedRandom = new SplittableRandom(0x891ea7f506edfc35L);
    long[] seeds = seedRandom.longs(numCycles).toArray();

    double[][][] estimatedDistinctCounts = new double[estimatorConfigs.size() + 1][][];
    for (int k = 0; k < estimatorConfigs.size() + 1; ++k) {
      estimatedDistinctCounts[k] = new double[targetDistinctCounts.size()][];
      for (int i = 0; i < targetDistinctCounts.size(); ++i) {
        estimatedDistinctCounts[k][i] = new double[numCycles];
      }
    }

    PseudoRandomGeneratorProvider prgProvider = PseudoRandomGeneratorProvider.splitMix64_V1();
    ThreadLocal<LocalState> localStates =
        ThreadLocal.withInitial(
            () -> new LocalState(supplier.get(), prgProvider.create(), hashGenerators, p));

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
                            final Transition[] transitions = state.transitions;
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

                              for (int k = 0; k < estimatorConfigs.size(); ++k) {
                                estimatedDistinctCounts[k][distinctCountIndex][i] =
                                    estimatorConfigs
                                        .get(k)
                                        .estimator
                                        .applyAsDouble(sketch, martingaleEstimator);
                              }
                            }
                          }))
          .get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }

    double[] theoreticalRelativeStandardErrors =
        estimatorConfigs.stream()
            .mapToDouble(c -> c.getpToAsymptoticRelativeStandardError().applyAsDouble(p))
            .toArray();

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

      for (EstimatorConfig estimatorConfig : estimatorConfigs) {
        writer.write("; relative bias " + estimatorConfig.getLabel());
        writer.write("; relative rmse " + estimatorConfig.getLabel());
        writer.write("; theoretical relative standard error " + estimatorConfig.getLabel());
      }
      writer.write('\n');

      for (int distinctCountIndex = 0;
          distinctCountIndex < targetDistinctCounts.size();
          ++distinctCountIndex) {

        double trueDistinctCount = targetDistinctCounts.get(distinctCountIndex).asDouble();
        writer.write("" + trueDistinctCount);

        for (int k = 0; k < estimatorConfigs.size(); ++k) {

          double relativeBias =
              DoubleStream.of(estimatedDistinctCounts[k][distinctCountIndex])
                      .map(x -> x - trueDistinctCount)
                      .sum()
                  / numCycles
                  / trueDistinctCount;
          double relativeRmse =
              Math.sqrt(
                      DoubleStream.of(estimatedDistinctCounts[k][distinctCountIndex])
                              .map(x -> (x - trueDistinctCount) * (x - trueDistinctCount))
                              .sum()
                          / numCycles)
                  / trueDistinctCount;

          writer.write("; " + relativeBias);
          writer.write("; " + relativeRmse);
          writer.write("; " + theoreticalRelativeStandardErrors[k]);
        }
        writer.write('\n');
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
