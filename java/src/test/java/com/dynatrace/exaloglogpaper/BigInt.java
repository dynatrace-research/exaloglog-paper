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

import static com.dynatrace.hash4j.util.Preconditions.checkArgument;

import com.dynatrace.hash4j.util.Preconditions;
import java.math.BigInteger;
import java.util.Objects;

public final class BigInt implements Comparable<BigInt> {

  private static final BigInteger TWO_POW_63 = BigInteger.valueOf(2).pow(63);
  private static final double TWO_POW_PLUS_63_DOUBLE = Math.pow(2., 63);

  private static final double TWO_POW_PLUS_126_DOUBLE = Math.pow(2., 126);
  private static final double TWO_POW_MINUS_63_DOUBLE = Math.pow(2., -63);

  private long low;
  private long high;

  public void increment() {
    low += 1;
    if (low < 0) {
      high += 1;
      low = 0;
    }
  }

  public void decrement() {
    low -= 1;
    if (low < 0) {
      high -= 1;
      low = Long.MAX_VALUE;
    }
  }

  public void add(BigInt other) {
    low += other.low;
    if (low < 0) {
      high += 1;
      low += 0x8000000000000000L;
    }
    high += other.high;
  }

  public void add(long x) {
    Preconditions.checkArgument(x >= 0);
    low += x;
    if (low < 0) {
      high += 1;
      low += 0x8000000000000000L;
    }
  }

  private BigInt(long high, long low) {
    checkArgument(high >= 0 && low >= 0);
    this.low = low;
    this.high = high;
  }

  public static BigInt floor(double d) {
    checkArgument(d >= 0.);
    return fromIntDouble(Math.floor(d));
  }

  public static BigInt ceil(double d) {
    checkArgument(d >= 0.);
    return fromIntDouble(Math.ceil(d));
  }

  private static BigInt fromIntDouble(double d) {
    checkArgument(d < TWO_POW_PLUS_126_DOUBLE);
    if (d >= TWO_POW_PLUS_63_DOUBLE) {
      long high = (long) (d * TWO_POW_MINUS_63_DOUBLE);
      long low = (long) (d - high * TWO_POW_PLUS_63_DOUBLE);
      return new BigInt(high, low);
    } else {
      return new BigInt(0L, (long) d);
    }
  }

  public double asDouble() {
    return high * TWO_POW_PLUS_63_DOUBLE + low;
  }

  public static BigInt fromBigInt(BigInteger i) {
    long low = i.mod(TWO_POW_63).longValueExact();
    long high = i.divide(TWO_POW_63).longValueExact();
    return new BigInt(high, low);
  }

  public static BigInt fromLong(long l) {
    checkArgument(l >= 0);
    return new BigInt(0, l);
  }

  public static BigInt createZero() {
    return new BigInt(0, 0);
  }

  @Override
  public String toString() {
    return BigInteger.valueOf(high).multiply(TWO_POW_63).add(BigInteger.valueOf(low)).toString();
  }

  @Override
  public int compareTo(BigInt o) {
    if (high > o.high) return 1;
    if (high < o.high) return -1;
    return Long.compare(low, o.low);
  }

  public boolean isPositive() {
    return low > 0 || high > 0;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof BigInt)) return false;
    BigInt bigInt = (BigInt) o;
    return low == bigInt.low && high == bigInt.high;
  }

  @Override
  public int hashCode() {
    return Objects.hash(low, high);
  }

  public void min(BigInt x) {
    if (high > x.high) {
      high = x.high;
      low = x.low;
    } else if (high == x.high && low > x.low) {
      low = x.low;
    }
  }

  public BigInt copy() {
    return new BigInt(high, low);
  }

  public void set(BigInt x) {
    this.high = x.high;
    this.low = x.low;
  }
}
