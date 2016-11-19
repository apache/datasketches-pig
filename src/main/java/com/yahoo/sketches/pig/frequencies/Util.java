/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.pig.frequencies;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.ArrayOfItemsSerDe;
import com.yahoo.sketches.frequencies.ItemsSketch;

final class Util {

  static final TupleFactory tupleFactory = TupleFactory.getInstance();

  static <T> Tuple serializeSketchToTuple(
      final ItemsSketch<T> sketch, final ArrayOfItemsSerDe<T> serDe) throws ExecException {
    Tuple outputTuple = Util.tupleFactory.newTuple(1);
    outputTuple.set(0, new DataByteArray(sketch.toByteArray(serDe)));
    return outputTuple;
  }

  static <T> ItemsSketch<T> deserializeSketchFromTuple(
      final Tuple tuple, final ArrayOfItemsSerDe<T> serDe) throws ExecException {
    byte[] bytes = ((DataByteArray) tuple.get(0)).get();
    return ItemsSketch.getInstance(new NativeMemory(bytes), serDe);
  }

}
