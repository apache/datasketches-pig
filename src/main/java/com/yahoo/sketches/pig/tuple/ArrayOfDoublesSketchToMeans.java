/*
 * Copyright 2017, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.pig.tuple;

import java.io.IOException;

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.tuple.ArrayOfDoublesSketch;
import com.yahoo.sketches.tuple.ArrayOfDoublesSketches;

/**
 * This UDF converts an ArrayOfDoubles sketch to mean values.
 * The result will be a tuple with N double values, where
 * N is the number of double values kept in the sketch per key.
 */
public class ArrayOfDoublesSketchToMeans extends EvalFunc<Tuple> {

  @Override
  public Tuple exec(final Tuple input) throws IOException {
    if ((input == null) || (input.size() == 0)) {
      return null;
    }

    final DataByteArray dba = (DataByteArray) input.get(0);
    final ArrayOfDoublesSketch sketch = ArrayOfDoublesSketches.wrapSketch(Memory.wrap(dba.get()));

    if (sketch.getRetainedEntries() < 1) {
      return null;
    }

    final SummaryStatistics[] summaries = ArrayOfDoublesSketchStats.sketchToSummaryStatistics(sketch);

    final Tuple means = TupleFactory.getInstance().newTuple(sketch.getNumValues());
    for (int i = 0; i < sketch.getNumValues(); i++) {
      means.set(i, summaries[i].getMean());
    }
    return means;
  }

}
