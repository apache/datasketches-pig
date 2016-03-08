/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.pig.tuple;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.yahoo.sketches.tuple.ArrayOfDoublesSketch;
import com.yahoo.sketches.tuple.ArrayOfDoublesSketchIterator;
import com.yahoo.sketches.tuple.ArrayOfDoublesSketches;
import com.yahoo.sketches.memory.NativeMemory;

/**
 * This UDF converts an ArrayOfDoubles sketch to an estimate and a bag of values.
 * Result: (estimate:double, bag:{(value, ...), (value, ...), ...})
 */
public class ArrayOfDoublesSketchToResult extends EvalFunc<Tuple> {
  @Override
  public Tuple exec(Tuple input) throws IOException {
    if ((input == null) || (input.size() == 0)) {
      return null;
    }

    DataByteArray dba = (DataByteArray) input.get(0);
    ArrayOfDoublesSketch sketch = ArrayOfDoublesSketches.heapifySketch(new NativeMemory(dba.get()));

    Tuple output = TupleFactory.getInstance().newTuple(2);
    output.set(0, sketch.getEstimate());
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    if (sketch.getRetainedEntries() > 0) { // remove unnecessary check when version of sketches-core > 0.4.0
      ArrayOfDoublesSketchIterator it = sketch.iterator();
      while (it.next()) {
        bag.add(Util.doubleArrayToTuple(it.getValues()));
      }
    }
    output.set(1,  bag);

    return output;
  }
}
