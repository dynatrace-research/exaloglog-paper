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

import static com.dynatrace.exaloglogpaper.EstimationErrorSimulationUtil.doSimulation;

import java.util.Arrays;

public class ExaLogLogEstimationErrorSimulation {
  public static void main(String[] args) {
    int t = Integer.parseInt(args[0]);
    int d = Integer.parseInt(args[1]);
    int p = Integer.parseInt(args[2]);
    String outputFile = args[3];
    doSimulation(
        t,
        d,
        p,
        "exaloglog",
        () -> ExaLogLog.create(t, d, p),
        Arrays.asList(
            new EstimationErrorSimulationUtil.EstimatorConfig(
                (s, m) -> m.getDistinctCountEstimate(),
                "martingale",
                pp -> PrecomputedConstants.getTheoreticalRelativeErrorMartingale(t, d, pp)),
            new EstimationErrorSimulationUtil.EstimatorConfig(
                (s, m) -> s.getDistinctCountEstimate(ExaLogLog.MAXIMUM_LIKELIHOOD_ESTIMATOR),
                "maximum likelihood",
                pp -> PrecomputedConstants.getTheoreticalRelativeErrorML(t, d, pp))),
        outputFile,
        TestUtils.getHashGenerators(p, t));
  }
}
