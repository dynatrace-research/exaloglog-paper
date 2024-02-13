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

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Test;

class MartingaleEstimatorTest {

  @Test
  void testToString() {
    assertThat(new MartingaleEstimator())
        .hasToString("MartingaleEstimator{distinctCountEstimate=0.0, stateChangeProbability=1.0}");
    assertThat(new MartingaleEstimator(2, 0.25))
        .hasToString("MartingaleEstimator{distinctCountEstimate=2.0, stateChangeProbability=0.25}");
  }

  @Test
  void testConstructorWithNegativeZeroStateChangeProbability() {
    MartingaleEstimator estimator = new MartingaleEstimator(0, -0.0);
    estimator.stateChanged(0.5);
    assertThat(estimator.getDistinctCountEstimate()).isPositive().isInfinite();
  }

  @Test
  void testConstructorWithIllegalArguments() {
    assertThatIllegalArgumentException().isThrownBy(() -> new MartingaleEstimator(-1, 1));
    assertThatIllegalArgumentException().isThrownBy(() -> new MartingaleEstimator(Double.NaN, 1));
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new MartingaleEstimator(Double.NEGATIVE_INFINITY, 1));
    assertThatIllegalArgumentException().isThrownBy(() -> new MartingaleEstimator(0, 2));
    assertThatIllegalArgumentException().isThrownBy(() -> new MartingaleEstimator(0, -1));
  }

  @Test
  void testBasicUsage() {
    MartingaleEstimator estimator = new MartingaleEstimator();
    assertThat(estimator.getDistinctCountEstimate()).isZero();
    assertThat(estimator.getStateChangeProbability()).isOne();
    for (int i = 1; i <= 100; ++i) {
      estimator.stateChanged(Math.pow(0.5, i));
      assertThat(estimator.getStateChangeProbability()).isEqualTo(Math.pow(0.5, i));
      assertThat(estimator.getDistinctCountEstimate()).isEqualTo(Math.pow(2., i) - 1.);
    }
  }

  @Test
  void testSet() {
    double distinctCountEstimate = 23478952;
    double stateChangeProbability = 0.823568;

    MartingaleEstimator martingaleEstimator = new MartingaleEstimator();
    martingaleEstimator.set(distinctCountEstimate, stateChangeProbability);

    assertThat(martingaleEstimator.getStateChangeProbability()).isEqualTo(stateChangeProbability);
    assertThat(martingaleEstimator.getDistinctCountEstimate()).isEqualTo(distinctCountEstimate);
  }

  @Test
  void testReset() {
    double distinctCountEstimate = 23478952;
    double stateChangeProbability = 0.823568;

    MartingaleEstimator martingaleEstimator =
        new MartingaleEstimator(distinctCountEstimate, stateChangeProbability);
    martingaleEstimator.reset();

    assertThat(martingaleEstimator.getStateChangeProbability()).isEqualTo(1.);
    assertThat(martingaleEstimator.getDistinctCountEstimate()).isEqualTo(0.);
  }

  @Test
  void testSetArguments() {
    MartingaleEstimator martingaleEstimator = new MartingaleEstimator();
    assertThatIllegalArgumentException().isThrownBy(() -> martingaleEstimator.set(-2, 0.5));
    assertThatIllegalArgumentException().isThrownBy(() -> martingaleEstimator.set(1, 1.5));
    assertThatNoException().isThrownBy(() -> martingaleEstimator.set(2, -0.0));
    assertThatIllegalArgumentException().isThrownBy(() -> martingaleEstimator.set(2, -0.1));
  }
}
