/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.pig.tuple;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.quantiles.DoublesSketch;
import com.yahoo.sketches.quantiles.DoublesSketchBuilder;
import com.yahoo.sketches.tuple.DoubleSummary;
import com.yahoo.sketches.tuple.Sketch;
import com.yahoo.sketches.tuple.SketchIterator;
import com.yahoo.sketches.tuple.Sketches;

/**
 * This UDF is to get a percentile value from a Sketch&lt;DoubleSummary&gt;.
 * The values from DoubleSummary objects in the sketch are extracted,
 * and a single value with the given rank is returned. The rank is in
 * percent. For example, 50th percentile is the median value of the
 * distribution (the number separating the higher half of a probability
 * distribution from the lower half).
 */
public class DoubleSummarySketchToPercentile extends EvalFunc<Double> {

  private static final int QUANTILES_SKETCH_SIZE = 1024;

  @Override
  public Double exec(final Tuple input) throws IOException {
    if (input.size() != 2) {
      throw new IllegalArgumentException("expected two inputs: sketch and pecentile");
    }

    final DataByteArray dba = (DataByteArray) input.get(0);
    final Sketch<DoubleSummary> sketch = Sketches.heapifySketch(new NativeMemory(dba.get()));

    final double percentile = (double) input.get(1);
    if (percentile < 0 || percentile > 100) {
      throw new IllegalArgumentException("percentile must be between 0 and 100");
    }

    final DoublesSketch qs = new DoublesSketchBuilder().build(QUANTILES_SKETCH_SIZE);
    final SketchIterator<DoubleSummary> it = sketch.iterator();
    while (it.next()) {
      qs.update(it.getSummary().getValue());
    }
    return qs.getQuantile(percentile / 100);
  }

}
