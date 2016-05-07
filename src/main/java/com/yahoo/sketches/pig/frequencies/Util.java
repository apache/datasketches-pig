/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.pig.frequencies;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.yahoo.sketches.frequencies.ArrayOfItemsSerDe;
import com.yahoo.sketches.frequencies.FrequentItemsSketch;
import com.yahoo.sketches.memory.NativeMemory;

final class Util {

  static final TupleFactory tupleFactory = TupleFactory.getInstance();

  static <T> Tuple serializeSketchToTuple(final FrequentItemsSketch<T> sketch, final ArrayOfItemsSerDe<T> serDe) throws ExecException {
    Tuple outputTuple = Util.tupleFactory.newTuple(1);
    outputTuple.set(0, new DataByteArray(sketch.serializeToByteArray(serDe)));
    return outputTuple;
  }

  static <T> FrequentItemsSketch<T> deserializeSketchFromTuple(final Tuple tuple, final ArrayOfItemsSerDe<T> serDe) throws ExecException {
    byte[] bytes = ((DataByteArray) tuple.get(0)).get();
    return FrequentItemsSketch.getInstance(new NativeMemory(bytes), serDe);
  }

}
