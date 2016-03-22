/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.pig.tuple;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.pig.Accumulator;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

import com.yahoo.sketches.tuple.ArrayOfDoublesSketch;
import com.yahoo.sketches.tuple.ArrayOfDoublesUpdatableSketchBuilder;
import com.yahoo.sketches.tuple.ArrayOfDoublesUnion;
import com.yahoo.sketches.tuple.ArrayOfDoublesSetOperationBuilder;

abstract class MergeArrayOfDoublesSketchBase extends EvalFunc<Tuple> implements Accumulator<Tuple> {
  private final int sketchSize_;
  private int numValues_;
  private ArrayOfDoublesUnion union_;
  private boolean isFirstCall_ = true;

  private static final ArrayOfDoublesSketch EMPTY_SKETCH = new ArrayOfDoublesUpdatableSketchBuilder().build().compact();

  MergeArrayOfDoublesSketchBase(int sketchSize, int numValues) {
    super();
    sketchSize_ = sketchSize;
    numValues_ = numValues;
  }

  @Override
  public Tuple exec(Tuple inputTuple) throws IOException {
    if (isFirstCall_) {
      Logger.getLogger(getClass()).info("exec is used"); // this is to see in the log which way was used by Pig
      isFirstCall_ = false;
    }
    if ((inputTuple == null) || (inputTuple.size() == 0)) {
      return null;
    }
    accumulate(inputTuple);
    Tuple outputTuple = getValue();
    cleanup();
    return outputTuple;
  }

  @Override
  public void accumulate(Tuple inputTuple) throws IOException {
    if (isFirstCall_) {
      Logger.getLogger(getClass()).info("accumulator is used"); // this is to see in the log which way was used by Pig
      isFirstCall_ = false;
    }
    if ((inputTuple == null) || (inputTuple.size() != 1)) {
      return;
    }
    Object obj = inputTuple.get(0);
    if (!(obj instanceof DataBag)) {
      return;
    }
    DataBag bag = (DataBag) inputTuple.get(0);
    if (bag.size() == 0) {
      return;
    }
  
    if (union_ == null) {
      union_ = new ArrayOfDoublesSetOperationBuilder().setNominalEntries(sketchSize_).setNumberOfValues(numValues_).buildUnion();
    }
    for (Tuple innerTuple: bag) {
      int sz = innerTuple.size();
      if ((sz != 1) || (innerTuple.get(0) == null)) {
        continue;
      }
      ArrayOfDoublesSketch incomingSketch = SerializerDeserializer.deserializeArrayOfDoublesSketchFromTuple(innerTuple);
      union_.update(incomingSketch);
    }
  }

  @Override
  public Tuple getValue() {
    if (union_ == null) { //return an empty sketch
      try {
        return SerializerDeserializer.serializeArrayOfDoublesSketchToTuple(EMPTY_SKETCH);
      } catch (ExecException ex) {
        throw new RuntimeException("Pig Error: " + ex.getMessage(), ex);
      }
    }
  
    try {
      ArrayOfDoublesSketch result = union_.getResult();
      return SerializerDeserializer.serializeArrayOfDoublesSketchToTuple(result);
    } catch (ExecException ex) {
      throw new RuntimeException("Pig Error: " + ex.getMessage(), ex);
    }
  }

  @Override
  public void cleanup() {
    if (union_ != null) union_.reset();
  }

}
