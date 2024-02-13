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

import java.math.BigInteger;
import org.junit.jupiter.api.Test;

class BigIntTest {

  @Test
  void testBigIntFloor() {
    assertThat(BigInt.floor(1.2)).hasToString("1");
    assertThat(BigInt.floor(1e20)).hasToString("100000000000000000000");
    assertThat(BigInt.floor(1e30)).hasToString("1000000000000000019884624838656");
  }

  @Test
  void testBigIntCeil() {
    assertThat(BigInt.ceil(1.2)).hasToString("2");
    assertThat(BigInt.ceil(1e20)).hasToString("100000000000000000000");
    assertThat(BigInt.ceil(1e30)).hasToString("1000000000000000019884624838656");
  }

  @Test
  void testIncrement() {
    BigInt i = BigInt.ceil(1);
    i.increment();
    assertThat(i).hasToString("2");
  }

  @Test
  void testDecrement() {
    BigInt i = BigInt.ceil(Math.pow(2, 63));
    i.decrement();
    assertThat(i).hasToString(Long.toString(Long.MAX_VALUE));
  }

  @Test
  void testAdd() {
    BigInt i1 = BigInt.fromBigInt(new BigInteger("10000500000000005"));
    BigInt i2 = BigInt.fromBigInt(new BigInteger("10000900000000006"));
    i1.add(i2);
    assertThat(i1).hasToString("20001400000000011");
  }
}
