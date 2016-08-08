/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.pig.quantiles;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

import com.yahoo.sketches.memory.NativeMemory;
import com.yahoo.sketches.quantiles.DoublesSketch;

/**
 * This UDF is to get a quantile value from a DoublesSketch. A single value for a
 * given fraction is returned. The fraction represents a normalized rank and must be
 * from 0 to 1 inclusive. For example, the fraction of 0.5 corresponds to 50th percentile,
 * which is the median value of the distribution (the number separating the higher half
 * of the probability distribution from the lower half).
 */
public class GetQuantileFromDoublesSketch extends EvalFunc<Double> {

  @Override
  public Double exec(final Tuple input) throws IOException {
    if (input.size() != 2) throw new IllegalArgumentException("expected two inputs: sketch and fraction");

    if (!(input.get(0) instanceof DataByteArray)) {
      throw new IllegalArgumentException("expected a DataByteArray as a sketch, got " 
          + input.get(0).getClass().getSimpleName());
    }
    final DataByteArray dba = (DataByteArray) input.get(0);
    final DoublesSketch sketch = DoublesSketch.heapify(new NativeMemory(dba.get()));

    if (!(input.get(1) instanceof Double)) {
      throw new IllegalArgumentException("expected a double value as a fraction, got " 
          + input.get(1).getClass().getSimpleName());
    }
    final double fraction = (double) input.get(1);
    return sketch.getQuantile(fraction);
  }

}
