/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.pig.tuple;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.yahoo.sketches.memory.NativeMemory;
import com.yahoo.sketches.tuple.DoubleSummary;
import com.yahoo.sketches.tuple.Sketch;
import com.yahoo.sketches.tuple.SketchIterator;
import com.yahoo.sketches.tuple.Sketches;

/**
 * This UDF converts a Sketch<DoubleSummary> to an estimate and an array of values
 * Result: (estimate:double, (value1, value2, ...))
 */
public class DoubleSummarySketchToResult extends EvalFunc<Tuple> {
  @Override
  public Tuple exec(Tuple input) throws IOException {
    if ((input == null) || (input.size() == 0)) {
      return null;
    }

    DataByteArray dba = (DataByteArray) input.get(0);
    Sketch<DoubleSummary> sketch = Sketches.heapifySketch(new NativeMemory(dba.get()));

    Tuple output = TupleFactory.getInstance().newTuple(2);
    output.set(0, sketch.getEstimate());
    double[] values = new double[sketch.getRetainedEntries()];
    SketchIterator<DoubleSummary> it = sketch.iterator();
    int i = 0;
    while (it.next()) {
      values[i++] = it.getSummary().getValue();
    }
    output.set(1, Util.doubleArrayToTuple(values));

    return output;
  }
}
