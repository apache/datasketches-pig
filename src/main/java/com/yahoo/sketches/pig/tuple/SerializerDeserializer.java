/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.pig.tuple;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.yahoo.sketches.tuple.Summary;
import com.yahoo.sketches.tuple.Sketch;
import com.yahoo.sketches.tuple.Sketches;
import com.yahoo.sketches.tuple.ArrayOfDoublesSketch;
import com.yahoo.sketches.tuple.ArrayOfDoublesSketches;
import com.yahoo.sketches.memory.NativeMemory;

class SerializerDeserializer {
  private static final TupleFactory tupleFactory = TupleFactory.getInstance();

  static Tuple serializeSketchToTuple(Sketch<?> sketch) throws ExecException {
    Tuple outputTuple = tupleFactory.newTuple(1);
    outputTuple.set(0, new DataByteArray(sketch.toByteArray()));
    return outputTuple;
  }

  static Tuple serializeArrayOfDoublesSketchToTuple(ArrayOfDoublesSketch sketch) throws ExecException {
    Tuple outputTuple = tupleFactory.newTuple(1);
    outputTuple.set(0, new DataByteArray(sketch.toByteArray()));
    return outputTuple;
  }

  static <S extends Summary> Sketch<S> deserializeSketchFromTuple(Tuple tuple) throws ExecException {
    byte[] bytes = ((DataByteArray) tuple.get(0)).get();
    return Sketches.heapifySketch(new NativeMemory(bytes));
  }

  static ArrayOfDoublesSketch deserializeArrayOfDoublesSketchFromTuple(Tuple tuple) throws ExecException {
    byte[] bytes = ((DataByteArray) tuple.get(0)).get();
    return ArrayOfDoublesSketches.heapifySketch(new NativeMemory(bytes));
  }
}
