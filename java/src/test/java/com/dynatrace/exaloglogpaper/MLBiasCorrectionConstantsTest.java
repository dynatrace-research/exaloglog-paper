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

import static com.dynatrace.exaloglogpaper.MLBiasCorrectionConstants.ML_BIAS_CORRECTION_CONSTANTS;
import static com.dynatrace.exaloglogpaper.TestUtils.hurvitzZeta;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.data.Percentage.withPercentage;

import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class MLBiasCorrectionConstantsTest {

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

  @ParameterizedTest
  @MethodSource("someExaLogLogConfigurations")
  void testSomeBiasCorrectionConstants(int t, int d) {
    assertThat(ML_BIAS_CORRECTION_CONSTANTS[t][d])
        .isCloseTo(calculateBiasCorrectionConstant(t, d), withPercentage(1e-5));
  }

  @Test
  void testDefinitionOfBiasCorrectionConstants() {
    assertThat(ML_BIAS_CORRECTION_CONSTANTS.length).isGreaterThanOrEqualTo(ExaLogLog.getMaxT() + 1);
    for (int t = 0; t <= ExaLogLog.getMaxT(); ++t) {
      assertThat(ML_BIAS_CORRECTION_CONSTANTS[t]).hasSize(ExaLogLog.getMaxD(t) + 1);
    }
  }
}
