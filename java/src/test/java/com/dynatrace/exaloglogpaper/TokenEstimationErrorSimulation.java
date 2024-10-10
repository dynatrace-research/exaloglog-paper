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
import java.util.Arrays;
import java.util.SplittableRandom;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

public class TokenEstimationErrorSimulation {

  private static final class LocalState {
    private final int tokens[];
    private final PseudoRandomGenerator prg;

    public LocalState(PseudoRandomGenerator prg, int maxDistinctCount) {
      this.prg = prg;
      this.tokens = new int[maxDistinctCount];
    }
  }

  private static DistinctCountUtil.TokenIterable fromSortedArray(int[] tokens, int tokenCount) {
    return () ->
        new DistinctCountUtil.TokenIterator() {
          private int idx = 0;

          @Override
          public boolean hasNext() {
            return idx < tokenCount;
          }

          @Override
          public int nextToken() {
            return tokens[idx++];
          }
        };
  }

  public static void main(String[] args) {
    int tokenParameter = Integer.parseInt(args[0]);
    String outputFile = args[1];

    // parameters
    int numCycles = 100_000;
    int maxDistinctCount = 100_000;
    int maxParallelism = 16;
    long[] targetDistinctCounts = TestUtils.getDistinctCountValues(1, maxDistinctCount, 0.05);

    SplittableRandom seedRandom = new SplittableRandom(0x9dcd409500d9acedL);
    long[] seeds = seedRandom.longs(numCycles).toArray();

    double[][] estimatedDistinctCounts = new double[targetDistinctCounts.length][];
    long[][] numIterations = new long[targetDistinctCounts.length][];
    for (int i = 0; i < targetDistinctCounts.length; ++i) {
      estimatedDistinctCounts[i] = new double[numCycles];
      numIterations[i] = new long[numCycles];
    }

    PseudoRandomGeneratorProvider prgProvider = PseudoRandomGeneratorProvider.splitMix64_V1();
    ThreadLocal<LocalState> localStates =
        ThreadLocal.withInitial(() -> new LocalState(prgProvider.create(), maxDistinctCount));

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

                            int[] tokens = state.tokens;
                            int tokenCounter = 0;

                            long trueDistinctCount = 0;
                            for (int distinctCountIndex = 0;
                                distinctCountIndex < targetDistinctCounts.length;
                                ++distinctCountIndex) {
                              long targetDistinctCount = targetDistinctCounts[distinctCountIndex];

                              while (trueDistinctCount < targetDistinctCount) {
                                tokens[tokenCounter++] =
                                    DistinctCountUtil.computeToken(prg.nextLong(), tokenParameter);
                                trueDistinctCount += 1;
                              }
                              Arrays.sort(tokens, 0, tokenCounter);
                              AtomicLong iterationCounter = new AtomicLong();
                              estimatedDistinctCounts[distinctCountIndex][i] =
                                  DistinctCountUtil.estimateDistinctCountFromTokens(
                                      fromSortedArray(tokens, tokenCounter),
                                      tokenParameter,
                                      iterationCounter);
                              numIterations[distinctCountIndex][i] = iterationCounter.get();
                            }
                          }))
          .get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }

    try (FileWriter writer = new FileWriter(outputFile, StandardCharsets.UTF_8)) {
      writer.write("token_parameter=" + tokenParameter + "; num_cycles=" + numCycles + "\n");

      writer.write("distinct count");
      writer.write("; relative bias");
      writer.write("; relative rmse");
      writer.write("; min num iterations");
      writer.write("; mean num iterations");
      writer.write("; max num iterations");
      writer.write('\n');

      for (int distinctCountIndex = 0;
          distinctCountIndex < targetDistinctCounts.length;
          ++distinctCountIndex) {

        double trueDistinctCount = targetDistinctCounts[distinctCountIndex];
        writer.write("" + trueDistinctCount);

        double relativeBias =
            TestUtils.calculateBias(estimatedDistinctCounts[distinctCountIndex], trueDistinctCount);
        double relativeRmse =
            TestUtils.calculateRmse(estimatedDistinctCounts[distinctCountIndex], trueDistinctCount);
        long minNumIterations = LongStream.of(numIterations[distinctCountIndex]).min().getAsLong();
        double meanNumIterations =
            LongStream.of(numIterations[distinctCountIndex]).average().getAsDouble();
        long maxNumIterations = LongStream.of(numIterations[distinctCountIndex]).max().getAsLong();

        writer.write("; " + relativeBias);
        writer.write("; " + relativeRmse);
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
