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
import com.yahoo.sketches.tuple.ArrayOfDoublesSketch;
import com.yahoo.sketches.tuple.ArrayOfDoublesSketchIterator;
import com.yahoo.sketches.tuple.ArrayOfDoublesSketches;

/**
 * This UDF converts an ArrayOfDoubles sketch to estimates.
 * The result will be a tuple with N + 1 double values, where
 * N is the number of double values kept in the sketch per key.
 * The first estimate is the estimate of the number of unique
 * keys in the original population.
 * Next there are N estimates of the sums of the parameters
 * in the original population (sums of the values in the sketch
 * scaled to the original population).
 */
public class ArrayOfDoublesSketchToEstimates extends EvalFunc<Tuple> {
  @Override
  public Tuple exec(final Tuple input) throws IOException {
    if ((input == null) || (input.size() == 0)) {
      return null;
    }

    final DataByteArray dba = (DataByteArray) input.get(0);
    final ArrayOfDoublesSketch sketch = ArrayOfDoublesSketches.wrapSketch(new NativeMemory(dba.get()));

    final double[] estimates = new double[sketch.getNumValues() + 1];
    estimates[0] = sketch.getEstimate();
    if (sketch.getRetainedEntries() > 0) { // remove unnecessary check when version of sketches-core > 0.4.0
      final ArrayOfDoublesSketchIterator it = sketch.iterator();
      while (it.next()) {
        final double[] values = it.getValues();
        for (int i = 0; i < sketch.getNumValues(); i++) {
          estimates[i + 1] += values[i];
        }
      }
      for (int i = 0; i < sketch.getNumValues(); i++) {
        estimates[i + 1] /= sketch.getTheta();
      }
    }
    return Util.doubleArrayToTuple(estimates);
  }
}
