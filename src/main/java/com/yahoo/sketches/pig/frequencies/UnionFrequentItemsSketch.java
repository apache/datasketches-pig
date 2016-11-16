/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.pig.frequencies;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.pig.Accumulator;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

import com.yahoo.sketches.ArrayOfItemsSerDe;
import com.yahoo.sketches.frequencies.ItemsSketch;

/**
 * This is a generic implementation to be specialized in concrete UDFs
 * @param <T> Type of item
 */
public abstract class UnionFrequentItemsSketch<T> extends EvalFunc<Tuple> implements Accumulator<Tuple> {
  private final int sketchSize_;
  private final ArrayOfItemsSerDe<T> serDe_;
  private ItemsSketch<T> sketch_;
  private boolean isFirstCall_ = true;

  /**
   * Constructs a function given a sketch size and serde
   * @param sketchSize parameter controlling the size of the sketch and the accuracy
   * @param serDe an instance of ArrayOfItemsSerDe to serialize and deserialize arrays of items
   */
  public UnionFrequentItemsSketch(final int sketchSize, final ArrayOfItemsSerDe<T> serDe) {
    super();
    sketchSize_ = sketchSize;
    serDe_ = serDe;
  }

  @Override
  public Tuple exec(final Tuple inputTuple) throws IOException {
    if (isFirstCall_) {
      // this is to see in the log which way was used by Pig
      Logger.getLogger(getClass()).info("exec is used");
      isFirstCall_ = false;
    }
    if ((inputTuple == null) || (inputTuple.size() == 0)) {
      return null;
    }
    accumulate(inputTuple);
    final Tuple outputTuple = getValue();
    cleanup();
    return outputTuple;
  }

  @Override
  public void accumulate(final Tuple inputTuple) throws IOException {
    if (isFirstCall_) {
      // this is to see in the log which way was used by Pig
      Logger.getLogger(getClass()).info("accumulator is used");
      isFirstCall_ = false;
    }
    if ((inputTuple == null) || (inputTuple.size() != 1)) {
      return;
    }
    final Object obj = inputTuple.get(0);
    if (!(obj instanceof DataBag)) {
      return;
    }
    final DataBag bag = (DataBag) inputTuple.get(0);
    if (bag.size() == 0) {
      return;
    }

    if (sketch_ == null) {
      sketch_ = new ItemsSketch<T>(sketchSize_);
    }
    for (final Tuple innerTuple: bag) {
      final int sz = innerTuple.size();
      if ((sz != 1) || (innerTuple.get(0) == null)) {
        continue;
      }
      final ItemsSketch<T> incomingSketch = Util.deserializeSketchFromTuple(innerTuple, serDe_);
      sketch_.merge(incomingSketch);
    }
  }

  @Override
  public Tuple getValue() {
    if (sketch_ == null) { //return an empty sketch
      try {
        return Util.serializeSketchToTuple(new ItemsSketch<T>(sketchSize_), serDe_);
      } catch (ExecException ex) {
        throw new RuntimeException("Pig Error: " + ex.getMessage(), ex);
      }
    }

    try {
      return Util.serializeSketchToTuple(sketch_, serDe_);
    } catch (ExecException ex) {
      throw new RuntimeException("Pig Error: " + ex.getMessage(), ex);
    }
  }

  @Override
  public void cleanup() {
    if (sketch_ != null) {
      sketch_.reset();
    }
  }

}
