/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.pig.frequencies;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.yahoo.sketches.frequencies.ArrayOfStringsSerDe;
import com.yahoo.sketches.frequencies.ErrorType;
import com.yahoo.sketches.frequencies.FrequentItemsSketch;
import com.yahoo.sketches.memory.NativeMemory;

/**
 * This UDF converts a FrequentItemsSketch&lt;String&gt; to estimates:
 * {(item, estimate, upper bound, lower bound), ...}
 */
public class FrequentStringsSketchToEstimates extends EvalFunc<DataBag> {
  @Override
  public DataBag exec(final Tuple input) throws IOException {
    if ((input == null) || (input.size() == 0)) {
      return null;
    }

    DataByteArray dba = (DataByteArray) input.get(0);
    final FrequentItemsSketch<String> sketch = FrequentItemsSketch.getInstance(new NativeMemory(dba.get()), new ArrayOfStringsSerDe());
    FrequentItemsSketch<String>.Row[] result = sketch.getFrequentItems(ErrorType.NO_FALSE_POSITIVES);

    final DataBag bag = BagFactory.getInstance().newDefaultBag();
    for (int i = 0; i < result.length; i++) {
      final Tuple tuple = TupleFactory.getInstance().newTuple(4);
      tuple.set(0, result[i].getItem());
      tuple.set(1, result[i].getEstimate());
      tuple.set(2, result[i].getUpperBound());
      tuple.set(3, result[i].getLowerBound());
      bag.add(tuple);
    }
    return bag;
  }
}
