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
import org.apache.pig.data.DataByteArray;
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
public abstract class UnionSketch<S extends Summary> extends EvalFunc<Tuple> implements Accumulator<Tuple> {
  private final int sketchSize_;
  private final SummaryFactory<S> summaryFactory_;
  private Union<S> union_;
  private boolean isFirstCall_ = true;

  public UnionSketch(final int sketchSize, final SummaryFactory<S> summaryFactory) {
    super();
    this.sketchSize_ = sketchSize;
    this.summaryFactory_ = summaryFactory;
  }

  @Override
  public Tuple exec(final Tuple inputTuple) throws IOException {
    if (isFirstCall_) {
      Logger.getLogger(getClass()).info("exec is used"); // this is to see in the log which way was used by Pig
      isFirstCall_ = false;
    }
    if ((inputTuple == null) || (inputTuple.size() == 0)) {
      return null;
    }
    final DataBag bag = (DataBag) inputTuple.get(0);
    final Union<S> union = new Union<S>(sketchSize_, summaryFactory_);
    updateUnion(bag, union);
    return Util.tupleFactory.newTuple(new DataByteArray(union.getResult().toByteArray()));
  }

  @Override
  public void accumulate(final Tuple inputTuple) throws IOException {
    if (isFirstCall_) {
      Logger.getLogger(getClass()).info("accumulator is used"); // this is to see in the log which way was used by Pig
      isFirstCall_ = false;
    }
    if ((inputTuple == null) || (inputTuple.size() != 1)) {
      return;
    }
    final DataBag bag = (DataBag) inputTuple.get(0);
    if (bag == null || bag.size() == 0) return;
    if (union_ == null) {
      union_ = new Union<S>(sketchSize_, summaryFactory_);
    }
    updateUnion(bag, union_);
  }

  @Override
  public Tuple getValue() {
    if (union_ == null) { //return an empty sketch
      return Util.tupleFactory.newTuple(new DataByteArray(Sketches.createEmptySketch().toByteArray()));
    }
    return Util.tupleFactory.newTuple(new DataByteArray(union_.getResult().toByteArray()));
  }

  @Override
  public void cleanup() {
    if (union_ != null) union_.reset();
  }

  private static <S extends Summary> void updateUnion(final DataBag bag, final Union<S> union) throws ExecException {
    for (final Tuple innerTuple: bag) {
      if ((innerTuple.size() != 1) || (innerTuple.get(0) == null)) {
        continue;
      }
      final Sketch<S> incomingSketch = Util.deserializeSketchFromTuple(innerTuple);
      union.update(incomingSketch);
    }
  }

}
