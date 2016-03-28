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

import com.yahoo.sketches.tuple.UpdatableSketch;
import com.yahoo.sketches.tuple.Union;
import com.yahoo.sketches.tuple.Sketch;
import com.yahoo.sketches.tuple.UpdatableSketchBuilder;
import com.yahoo.sketches.tuple.UpdatableSummary;
import com.yahoo.sketches.tuple.SummaryFactory;

/**
 * Class used to calculate the intermediate pass (combiner) or the final pass
 * (reducer) of an Algebraic sketch operation. This may be called multiple times
 * (from the mapper and from the reducer). It will receive a bag of values
 * returned by either the Intermediate stage or the Initial stages, so
 * it needs to be able to differentiate between and interpret both types.
 * @param <U> Type of the update value
 * @param <S> Type of the summary
 */
abstract class DataToSketchAlgebraicIntermediateFinal<U, S extends UpdatableSummary<U>> extends EvalFunc<Tuple> {
  private int sketchSize_;
  private float samplingProbability_;
  private SummaryFactory<S> summaryFactory_;
  private boolean isFirstCall_ = true;

  /**
   * Default constructor to make pig validation happy.
   */
  DataToSketchAlgebraicIntermediateFinal() {}
  
  DataToSketchAlgebraicIntermediateFinal(int sketchSize, SummaryFactory<S> summaryFactory) {
    this(sketchSize, 1f, summaryFactory);
  }
  
  DataToSketchAlgebraicIntermediateFinal(int sketchSize, float samplingProbability, SummaryFactory<S> summaryFactory) {
    this.sketchSize_ = sketchSize;
    this.samplingProbability_ = samplingProbability;
    this.summaryFactory_ = summaryFactory;
  }
  
  @Override
  public Tuple exec(Tuple inputTuple) throws IOException {
    if (isFirstCall_) {
      Logger.getLogger(getClass()).info("algebraic is used"); // this is to see in the log which way was used by Pig
      isFirstCall_ = false;
    }
    UpdatableSketch<U, S> sketch = null;
    Union<S> union = new Union<S>(sketchSize_, summaryFactory_);

    DataBag bag = (DataBag) inputTuple.get(0);
    if (bag == null) {
      throw new IllegalArgumentException("InputTuple.Field0: Bag may not be null");
    }

    for (Tuple dataTuple: bag) {
      Object item = dataTuple.get(0);
      if (item instanceof DataBag) {
        // this is a bag from the Initial function.
        // just insert each item of the tuple into the sketch
        if (sketch == null) sketch = new UpdatableSketchBuilder<U, S>(summaryFactory_).setNominalEntries(sketchSize_).setSamplingProbability(samplingProbability_).build();
        DataToSketch.updateSketch((DataBag)item, sketch);
      } else if (item instanceof DataByteArray) {
        // This is a sketch from a prior call to the 
        // Intermediate function. merge it with the 
        // current sketch.
        Sketch<S> incomingSketch = Util.deserializeSketchFromTuple(dataTuple);
        if (incomingSketch.isEmpty()) continue;
        union.update(incomingSketch);
      } else {
        // we should never get here.
        throw new IllegalArgumentException("InputTuple.Field0: Bag contains unrecognized types: " + item.getClass().getName());
      }
    }
    if (sketch != null) union.update(sketch);
    return Util.serializeSketchToTuple(union.getResult());
  }
}
