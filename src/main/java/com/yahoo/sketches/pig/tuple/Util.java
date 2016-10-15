/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.pig.tuple;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.tuple.Sketch;
import com.yahoo.sketches.tuple.Sketches;
import com.yahoo.sketches.tuple.Summary;

final class Util {

  static final TupleFactory tupleFactory = TupleFactory.getInstance();

  static Tuple doubleArrayToTuple(final double[] array) throws ExecException {
    final Tuple tuple = tupleFactory.newTuple(array.length);
    for (int i = 0; i < array.length; i++) {
      tuple.set(i, array[i]);
    }
    return tuple;
  }

  static <S extends Summary> Sketch<S> deserializeSketchFromTuple(final Tuple tuple) throws ExecException {
    final byte[] bytes = ((DataByteArray) tuple.get(0)).get();
    return Sketches.heapifySketch(new NativeMemory(bytes));
  }

}
