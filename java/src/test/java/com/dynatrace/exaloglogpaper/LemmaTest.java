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

import static com.dynatrace.exaloglogpaper.TestUtils.POW_0_5;
import static com.dynatrace.exaloglogpaper.TestUtils.phi;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/** Unit test to verify Lemmma 1 in the paper. */
public class LemmaTest {

  private static double rhoUpdate(int k, int p, int t) {
    return POW_0_5[phi(k, p, t)];
  }

  private static double omega(int u, int p, int t) {
    int phiEval = phi(u, p, t);
    return ((1L << t) * (1L - t + phiEval) - u) * POW_0_5[phiEval];
  }

  @Test
  void testOmegaIdentity() {
    for (int p = 2; p <= 40; ++p) {
      for (int t = 0; t <= 8; ++t) {
        for (int u = 1; u <= (65 - p - t) << t; ++u) {
          double sum = 0;
          for (int k = (65 - p - t) << t; k >= u + 1; k--) {
            sum += rhoUpdate(k, p, t);
          }
          assertThat(omega(u, p, t)).isEqualTo(sum);
        }
      }
    }
  }
}
