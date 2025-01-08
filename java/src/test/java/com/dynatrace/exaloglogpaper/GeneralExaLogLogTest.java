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

import static com.dynatrace.exaloglogpaper.ExaLogLog.*;
import static org.assertj.core.api.Assertions.*;

import java.util.SplittableRandom;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class GeneralExaLogLogTest {

  @Test
  void testValidParameters() {
    for (int t = 0; t <= getMaxT(); ++t) {
      for (int p = getMinP(); p <= getMaxP(t); ++p) {
        for (int d = 0; d <= getMaxD(t); ++d) {
          ExaLogLog exaloglog = ExaLogLog.create(t, d, p);
          assertThat(exaloglog.getT()).isEqualTo(t);
          assertThat(exaloglog.getP()).isEqualTo(p);
          assertThat(exaloglog.getD()).isEqualTo(d);
        }
      }
    }
  }

  @Test
  void testInvalidParameters() {

    assertThatIllegalArgumentException()
        .isThrownBy(() -> ExaLogLog.create(getMaxT() + 1, 0, getMinP()));
    assertThatIllegalArgumentException().isThrownBy(() -> ExaLogLog.create(-1, 0, getMinP()));
    for (int t = 0; t < getMaxT(); ++t) {
      int finalT = t;
      assertThatIllegalArgumentException()
          .isThrownBy(() -> ExaLogLog.create(finalT, 0, getMinP() - 1));
      assertThatIllegalArgumentException()
          .isThrownBy(() -> ExaLogLog.create(finalT, 0, getMaxP(finalT) + 1));
      assertThatIllegalArgumentException()
          .isThrownBy(() -> ExaLogLog.create(finalT, -1, getMinP()));
      assertThatIllegalArgumentException()
          .isThrownBy(() -> ExaLogLog.create(finalT, getMaxD(finalT) + 1, getMinP()));
    }
  }

  @Test
  void testAddWithDifferentParameters() {
    {
      ExaLogLog exaLogLog1 = ExaLogLog.create(1, 0, 4);
      ExaLogLog exaLogLog2 = ExaLogLog.create(2, 0, 4);

      assertThatIllegalArgumentException().isThrownBy(() -> exaLogLog2.add(exaLogLog1));
      assertThatIllegalArgumentException().isThrownBy(() -> exaLogLog1.add(exaLogLog2));
    }
    {
      ExaLogLog exaLogLog1 = ExaLogLog.create(2, 2, 4);
      ExaLogLog exaLogLog2 = ExaLogLog.create(2, 3, 4);

      assertThatIllegalArgumentException().isThrownBy(() -> exaLogLog2.add(exaLogLog1));
      assertThatNoException().isThrownBy(() -> exaLogLog1.add(exaLogLog2));
    }
    {
      ExaLogLog exaLogLog1 = ExaLogLog.create(2, 3, 4);
      ExaLogLog exaLogLog2 = ExaLogLog.create(2, 3, 5);

      assertThatIllegalArgumentException().isThrownBy(() -> exaLogLog2.add(exaLogLog1));
      assertThatNoException().isThrownBy(() -> exaLogLog1.add(exaLogLog2));
    }
    {
      ExaLogLog exaLogLog1 = ExaLogLog.create(2, 2, 4);
      ExaLogLog exaLogLog2 = ExaLogLog.create(2, 3, 5);

      assertThatIllegalArgumentException().isThrownBy(() -> exaLogLog2.add(exaLogLog1));
      assertThatNoException().isThrownBy(() -> exaLogLog1.add(exaLogLog2));
    }
    {
      ExaLogLog exaLogLog1 = ExaLogLog.create(2, 3, 4);
      ExaLogLog exaLogLog2 = ExaLogLog.create(2, 2, 5);

      assertThatIllegalArgumentException().isThrownBy(() -> exaLogLog2.add(exaLogLog1));
      assertThatIllegalArgumentException().isThrownBy(() -> exaLogLog1.add(exaLogLog2));
    }
  }

  @Test
  void testMergeAndDownsize() {
    int[] tValues = IntStream.range(0, 5).toArray();
    int[] pValues = IntStream.range(getMinP(), 8).toArray();
    int[] dValues = IntStream.range(0, 10).toArray();

    SplittableRandom random = new SplittableRandom(0xb6b2b7c2032f2513L);

    int numIterations = 100;

    for (int i = 0; i < numIterations; ++i) {
      int t = tValues[random.nextInt(tValues.length)];
      int p1 = pValues[random.nextInt(pValues.length)];
      int p2 = pValues[random.nextInt(pValues.length)];
      int d1 = dValues[random.nextInt(dValues.length)];
      int d2 = dValues[random.nextInt(dValues.length)];

      ExaLogLog exaLogLog1 = ExaLogLog.create(t, d1, p1);
      ExaLogLog exaLogLog2 = ExaLogLog.create(t, d2, p2);

      long distinctCount = random.nextLong(1000);
      for (long l = 0; l < distinctCount; ++l) {
        long hash = random.nextLong();
        exaLogLog1.add(hash);
        exaLogLog2.add(hash);
      }

      int minP = Math.min(p1, p2);
      int minD = Math.min(d1, d2);
      ExaLogLog exaLogLogMerged = ExaLogLog.merge(exaLogLog1, exaLogLog2);
      ExaLogLog exaLogLogDownSized1 = exaLogLog1.downsize(minD, minP);
      ExaLogLog exaLogLogDownSized2 = exaLogLog2.downsize(minD, minP);

      assertThat(exaLogLogMerged.getState())
          .isEqualTo(exaLogLogDownSized1.getState())
          .isEqualTo(exaLogLogDownSized2.getState());
    }
  }

  @Test
  void testMergeWithDifferentT() {
    ExaLogLog ell1 = ExaLogLog.create(2, 2, 2);
    ExaLogLog ell2 = ExaLogLog.create(3, 2, 2);
    assertThatIllegalArgumentException().isThrownBy(() -> ExaLogLog.merge(ell1, ell2));
  }

  private static int phi(long k, int p, int t) {
    return (int) Math.min((t + 1 + ((k - 1) >>> t)), 64 - p);
  }

  // corresponds to omega * 2^(64 - p)
  private static long omegaScaled(long u, int p, int t) {
    int phiEval = phi(u, p, t);
    return (((1L - t + phiEval) << t) - u) << (-p - phiEval);
  }

  private static long contributeReference(long r, int[] b, int p, int t, int d) {
    long u = r >>> d;
    if (u == 0) return (1L << -p);
    long a = omegaScaled(u, p, t);
    for (long k = Math.max(1, u - d); k <= u; ++k) {
      boolean unset = (u != k) && ((r & (1L << (d - u + k))) == 0);
      if (unset) {
        a += 1L << (-p - phi(k, p, t));
      } else if (b != null) {
        b[phi(k, p, t) - t - 1] += 1;
      }
    }
    return a;
  }

  private static void verifyContribute(long r, int t, int p, int d) {
    int[] b = new int[64];
    int[] bRef = new int[64];
    long a = ExaLogLog.contribute(r, b, t, d, p);
    long a2 = ExaLogLog.contribute(r, null, t, d, p);
    long aRef = contributeReference(r, bRef, p, t, d);
    assertThat(a).isEqualTo(aRef);
    assertThat(a2).isEqualTo(aRef);
    assertThat(b).containsExactly(bRef);
  }

  private static void verifyContribute(int t, int p, int d) {
    int numCycles = 1;
    SplittableRandom random = new SplittableRandom(0);
    for (long u = 0; u <= ((65L - p - t) << t); ++u) {
      for (int i = 0; i < numCycles; ++i) {
        long r = (u << d) | (random.nextLong() >>> 1 >>> ~d);
        verifyContribute(r, t, p, d);
      }
    }
  }

  @Test
  void testContribute() {
    for (int t = 0; t <= 8; ++t) {
      for (int d = 0; d <= ExaLogLog.getMaxD(t); ++d) {
        for (int p = ExaLogLog.getMinP(); p <= 8; ++p) {
          verifyContribute(t, p, d);
        }
      }
    }
  }
}
