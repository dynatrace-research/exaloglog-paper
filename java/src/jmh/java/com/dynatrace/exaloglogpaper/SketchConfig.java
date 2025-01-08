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

import com.dynatrace.hash4j.distinctcount.HyperLogLog;
import com.dynatrace.hash4j.distinctcount.UltraLogLog;
import com.dynatrace.hash4j.hashing.Hasher64;
import com.dynatrace.hash4j.hashing.Hashing;
import java.util.Arrays;
import org.apache.datasketches.cpc.CpcSketch;
import org.apache.datasketches.cpc.CpcUnion;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.TgtHllType;
import org.apache.datasketches.hll.Union;

public enum SketchConfig {
  HYPERLOGLOG_11 {

    @Override
    public Object createEmptySketch() {
      return HyperLogLog.create(11);
    }

    @Override
    public void add(Object sketch, byte[] data) {
      ((HyperLogLog) sketch).add(HASHER.hashBytesToLong(data));
    }

    @Override
    public double estimate(Object sketch) {
      return ((HyperLogLog) sketch)
          .getDistinctCountEstimate(HyperLogLog.MAXIMUM_LIKELIHOOD_ESTIMATOR);
    }

    @Override
    public byte[] serialize(Object sketch) {
      byte[] state = ((HyperLogLog) sketch).getState();
      return Arrays.copyOf(state, state.length);
    }

    @Override
    public Object merge(Object sketch1, Object sketch2) {
      return ((HyperLogLog) sketch1).copy().add((HyperLogLog) sketch2);
    }
  },
  ULTRALOGLOG_10 {
    @Override
    public Object createEmptySketch() {
      return UltraLogLog.create(10);
    }

    @Override
    public void add(Object sketch, byte[] data) {
      ((UltraLogLog) sketch).add(HASHER.hashBytesToLong(data));
    }

    @Override
    public double estimate(Object sketch) {
      return ((UltraLogLog) sketch)
          .getDistinctCountEstimate(UltraLogLog.MAXIMUM_LIKELIHOOD_ESTIMATOR);
    }

    @Override
    public byte[] serialize(Object sketch) {
      byte[] state = ((UltraLogLog) sketch).getState();
      return Arrays.copyOf(state, state.length);
    }

    @Override
    public Object merge(Object sketch1, Object sketch2) {
      return ((UltraLogLog) sketch1).copy().add((UltraLogLog) sketch2);
    }
  },
  EXALOGLOG_2_24_8 {
    @Override
    public Object createEmptySketch() {
      return ExaLogLog.create(2, 24, 8);
    }

    @Override
    public void add(Object sketch, byte[] data) {
      ((ExaLogLog) sketch).add(HASHER.hashBytesToLong(data));
    }

    @Override
    public double estimate(Object sketch) {
      return ((ExaLogLog) sketch).getDistinctCountEstimate();
    }

    @Override
    public byte[] serialize(Object sketch) {
      byte[] state = ((ExaLogLog) sketch).getState();
      return Arrays.copyOf(state, state.length);
    }

    @Override
    public Object merge(Object sketch1, Object sketch2) {
      return ((ExaLogLog) sketch1).copy().add((ExaLogLog) sketch2);
    }
  },
  EXALOGLOG_2_20_8 {
    @Override
    public Object createEmptySketch() {
      return ExaLogLog.create(2, 20, 8);
    }

    @Override
    public void add(Object sketch, byte[] data) {
      ((ExaLogLog) sketch).add(HASHER.hashBytesToLong(data));
    }

    @Override
    public double estimate(Object sketch) {
      return ((ExaLogLog) sketch).getDistinctCountEstimate();
    }

    @Override
    public byte[] serialize(Object sketch) {
      byte[] state = ((ExaLogLog) sketch).getState();
      return Arrays.copyOf(state, state.length);
    }

    @Override
    public Object merge(Object sketch1, Object sketch2) {
      return ((ExaLogLog) sketch1).copy().add((ExaLogLog) sketch2);
    }
  },
  EXALOGLOG_2_20_8_MARTINGALE {

    record Sketch(ExaLogLog sketch, MartingaleEstimator martingaleEstimator) {
      void add(byte[] data) {
        this.sketch.add(HASHER.hashBytesToLong(data), Sketch.this.martingaleEstimator);
      }
    }

    @Override
    public Object createEmptySketch() {
      return new Sketch(ExaLogLog.create(2, 20, 8), new MartingaleEstimator());
    }

    @Override
    public void add(Object sketch, byte[] data) {
      ((Sketch) sketch).add(data);
    }

    @Override
    public double estimate(Object sketch) {
      return ((Sketch) sketch).martingaleEstimator().getDistinctCountEstimate();
    }

    @Override
    public byte[] serialize(Object sketch) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Object merge(Object sketch1, Object sketch2) {
      throw new UnsupportedOperationException();
    }
  },
  EXALOGLOG_2_24_8_MARTINGALE {

    record Sketch(ExaLogLog sketch, MartingaleEstimator martingaleEstimator) {
      void add(byte[] data) {
        this.sketch.add(HASHER.hashBytesToLong(data), Sketch.this.martingaleEstimator);
      }
    }

    @Override
    public Object createEmptySketch() {
      return new Sketch(ExaLogLog.create(2, 24, 8), new MartingaleEstimator());
    }

    @Override
    public void add(Object sketch, byte[] data) {
      ((Sketch) sketch).add(data);
    }

    @Override
    public double estimate(Object sketch) {
      return ((Sketch) sketch).martingaleEstimator().getDistinctCountEstimate();
    }

    @Override
    public byte[] serialize(Object sketch) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Object merge(Object sketch1, Object sketch2) {
      throw new UnsupportedOperationException();
    }
  },
  APACHE_DATA_SKETCHES_CPC_10 {
    @Override
    public Object createEmptySketch() {
      return new CpcSketch(10);
    }

    @Override
    public void add(Object sketch, byte[] data) {
      ((CpcSketch) sketch).update(data);
    }

    @Override
    public double estimate(Object sketch) {
      return ((CpcSketch) sketch).getEstimate();
    }

    @Override
    public byte[] serialize(Object sketch) {
      return ((CpcSketch) sketch).toByteArray();
    }

    @Override
    public Object merge(Object sketch1, Object sketch2) {
      CpcSketch cpc1 = (CpcSketch) sketch1;
      CpcSketch cpc2 = (CpcSketch) sketch2;
      CpcUnion union = new CpcUnion(cpc1.getLgK());
      union.update(cpc1);
      union.update(cpc2);
      return union.getResult();
    }
  },
  APACHE_DATA_SKETCHES_HLL4_11 {
    @Override
    public Object createEmptySketch() {
      return new HllSketch(11, TgtHllType.HLL_4);
    }

    @Override
    public void add(Object sketch, byte[] data) {
      ((HllSketch) sketch).update(data);
    }

    @Override
    public double estimate(Object sketch) {
      return ((HllSketch) sketch).getEstimate();
    }

    @Override
    public byte[] serialize(Object sketch) {
      return ((HllSketch) sketch).toCompactByteArray();
    }

    @Override
    public Object merge(Object sketch1, Object sketch2) {
      HllSketch hll1 = (HllSketch) sketch1;
      HllSketch hll2 = (HllSketch) sketch2;
      Union union = new Union(hll1.getLgConfigK());
      union.update(hll1);
      union.update(hll2);
      return union.getResult();
    }
  },
  APACHE_DATA_SKETCHES_HLL6_11 {
    @Override
    public Object createEmptySketch() {
      return new HllSketch(11, TgtHllType.HLL_6);
    }

    @Override
    public void add(Object sketch, byte[] data) {
      ((HllSketch) sketch).update(data);
    }

    @Override
    public double estimate(Object sketch) {
      return ((HllSketch) sketch).getEstimate();
    }

    @Override
    public byte[] serialize(Object sketch) {
      return ((HllSketch) sketch).toCompactByteArray();
    }

    @Override
    public Object merge(Object sketch1, Object sketch2) {
      HllSketch hll1 = (HllSketch) sketch1;
      HllSketch hll2 = (HllSketch) sketch2;
      Union union = new Union(hll1.getLgConfigK());
      union.update(hll1);
      union.update(hll2);
      return union.getResult();
    }
  },
  APACHE_DATA_SKETCHES_HLL8_11 {
    @Override
    public Object createEmptySketch() {
      return new HllSketch(11, TgtHllType.HLL_8);
    }

    @Override
    public void add(Object sketch, byte[] data) {
      ((HllSketch) sketch).update(data);
    }

    @Override
    public double estimate(Object sketch) {
      return ((HllSketch) sketch).getEstimate();
    }

    @Override
    public byte[] serialize(Object sketch) {
      return ((HllSketch) sketch).toCompactByteArray();
    }

    @Override
    public Object merge(Object sketch1, Object sketch2) {
      HllSketch hll1 = (HllSketch) sketch1;
      HllSketch hll2 = (HllSketch) sketch2;
      Union union = new Union(hll1.getLgConfigK());
      union.update(hll1);
      union.update(hll2);
      return union.getResult();
    }
  };

  private static final Hasher64 HASHER = Hashing.murmur3_128();

  public abstract Object createEmptySketch();

  public abstract void add(Object sketch, byte[] data);

  public abstract double estimate(Object sketch);

  public abstract byte[] serialize(Object sketch);

  public abstract Object merge(Object sketch1, Object sketch2);
}
