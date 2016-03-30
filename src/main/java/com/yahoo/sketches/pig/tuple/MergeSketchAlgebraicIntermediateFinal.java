/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.pig.tuple;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

import com.yahoo.sketches.tuple.Union;
import com.yahoo.sketches.tuple.Sketch;
import com.yahoo.sketches.tuple.Summary;
import com.yahoo.sketches.tuple.SummaryFactory;

/**
 * This is to calculate the intermediate pass (combiner) or the final pass
 * (reducer) of an Algebraic sketch operation. This may be called multiple times
 * (from the mapper and from the reducer). It will receive a bag of values
 * returned by either the Intermediate or the Initial stages, so
 * it needs to be able to differentiate between and interpret both types.
 * 
 * @param <S> Type of Summary
 */
abstract class MergeSketchAlgebraicIntermediateFinal<S extends Summary> extends EvalFunc<Tuple> {
  private int sketchSize_;
  private SummaryFactory<S> summaryFactory_;
  private boolean isFirstCall_ = true;

  MergeSketchAlgebraicIntermediateFinal() {}

  MergeSketchAlgebraicIntermediateFinal(int sketchSize, SummaryFactory<S> summaryFactory) {
    this.sketchSize_ = sketchSize;
    this.summaryFactory_ = summaryFactory;
  }

  @Override
  public Tuple exec(Tuple inputTuple) throws IOException {
    if (isFirstCall_) {
      Logger.getLogger(getClass()).info("algebraic is used");  // this is to see in the log which way was used by Pig
      isFirstCall_ = false;
    }
    Union<S> union = new Union<S>(sketchSize_, summaryFactory_);

    DataBag bag = (DataBag) inputTuple.get(0);
    if (bag == null) throw new IllegalArgumentException("InputTuple.Field0: Bag may not be null");

    for (Tuple dataTuple: bag) {
      Object item = dataTuple.get(0);
      if (item instanceof DataBag) {
        // this is from a prior call to the initial function, so there is a nested bag.
        for (Tuple innerTuple: (DataBag) item) {
          Sketch<S> incomingSketch = Util.deserializeSketchFromTuple(innerTuple);
          if (incomingSketch.isEmpty()) continue;
          union.update(incomingSketch);
        }
      } else if (item instanceof DataByteArray) {
        // This is a sketch from a call to the Intermediate function 
        // Merge it with the current sketch.
        Sketch<S> incomingSketch = Util.deserializeSketchFromTuple(dataTuple);
        if (incomingSketch.isEmpty()) continue;
        union.update(incomingSketch);
      } else {
        // we should never get here.
        throw new IllegalArgumentException("InputTuple.Field0: Bag contains unrecognized types: " + item.getClass().getName());
      }
    }
    Sketch<S> result = union.getResult();
    return Util.serializeSketchToTuple(result);
  }
}
