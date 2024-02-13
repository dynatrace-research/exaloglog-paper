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

import static org.assertj.core.api.Assertions.assertThat;

import com.dynatrace.hash4j.hashing.HashStream64;
import com.dynatrace.hash4j.hashing.Hashing;
import java.util.SplittableRandom;
import org.junit.jupiter.api.Test;

public class ExaLogLog_2_20Test extends ExaLogLogTest {

  private static final int T = 2;
  private static final int D = 20;

  @Override
  protected ExaLogLog create(int p) {
    return ExaLogLog.create(T, D, p);
  }

  @Override
  protected ExaLogLog wrap(byte[] state) {
    return ExaLogLog.wrap(T, D, state);
  }

  @Override
  protected int getT() {
    return T;
  }

  @Override
  protected int getD() {
    return D;
  }

  @Test
  void testStability() {

    SplittableRandom random = new SplittableRandom(0);
    HashStream64 x = Hashing.komihash5_0().hashStream();
    for (int i = 0; i < 1000; ++i) {
      long distinctCount = random.nextLong(0L, 10000L);
      ExaLogLog exaLogLog = ExaLogLog.create(T, D, 4);
      for (long j = 0; j < distinctCount; ++j) {
        exaLogLog.add(distinctCount);
      }
      x.putDouble(exaLogLog.getDistinctCountEstimate());
    }
    long hash = x.getAsLong();
    assertThat(hash).isEqualTo(0x54257ae9db40a491L);
  }

  @Override
  protected long getCompatibilityFingerPrint() {
    return 0xc07e26536455a7ddL;
  }
}
