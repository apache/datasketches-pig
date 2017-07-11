/*
 * Copyright 2017, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.pig.hll;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

import com.yahoo.sketches.hll.HllSketch;

/**
 * This is a User Defined Function (UDF) for getting a unique count estimate from an HllSketch
 *
 * @author Alexander Saydakov
 */
public class SketchToEstimate extends EvalFunc<Double> {

  @Override
  public Double exec(final Tuple sketchTuple) throws IOException {
    if ((sketchTuple == null) || (sketchTuple.size() == 0)) {
      return null;
    }
    final DataByteArray dba = (DataByteArray) sketchTuple.get(0);
    final HllSketch sketch = HllSketch.heapify(dba.get());
    return sketch.getEstimate();
  }

}
