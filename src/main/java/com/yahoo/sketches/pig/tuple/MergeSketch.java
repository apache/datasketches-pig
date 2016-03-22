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

import com.yahoo.sketches.tuple.Union;
import com.yahoo.sketches.tuple.Sketch;
import com.yahoo.sketches.tuple.Sketches;
import com.yahoo.sketches.tuple.Summary;
import com.yahoo.sketches.tuple.SummaryFactory;

/**
 * This is a generic implementation to be specialized in concrete UDFs 
 * @param <S> Summary type
 */
abstract class MergeSketch<S extends Summary> extends EvalFunc<Tuple> implements Accumulator<Tuple> {
  private final int sketchSize_;
  private final SummaryFactory<S> summaryFactory_;
  private Union<S> union_;
  private boolean isFirstCall_ = true;

  MergeSketch(int sketchSize, SummaryFactory<S> summaryFactory) {
    super();
    this.sketchSize_ = sketchSize;
    this.summaryFactory_ = summaryFactory;
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
      union_ = new Union<S>(sketchSize_, summaryFactory_);
    }
    for (Tuple innerTuple: bag) {
      int sz = innerTuple.size();
      if ((sz != 1) || (innerTuple.get(0) == null)) {
        continue;
      }
      Sketch<S> incomingSketch = Util.deserializeSketchFromTuple(innerTuple);
      union_.update(incomingSketch);
    }
  }

  @Override
  public Tuple getValue() {
    if (union_ == null) { //return an empty sketch
      try {
        return Util.serializeSketchToTuple(Sketches.createEmptySketch());
      } catch (ExecException ex) {
        throw new RuntimeException("Pig Error: " + ex.getMessage(), ex);
      }
    }
  
    try {
      Sketch<S> result = union_.getResult();
      return Util.serializeSketchToTuple(result);
    } catch (ExecException ex) {
      throw new RuntimeException("Pig Error: " + ex.getMessage(), ex);
    }
  }

  @Override
  public void cleanup() {
    if (union_ != null) union_.reset();
  }

}
