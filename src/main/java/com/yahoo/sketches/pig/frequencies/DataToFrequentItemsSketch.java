/*
 * Copyright 2015, Yahoo! Inc.
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
 * @param <T> type of item
 */
public abstract class DataToFrequentItemsSketch<T> extends EvalFunc<Tuple> implements Accumulator<Tuple> {

  private final int sketchSize_;
  private ItemsSketch<T> accumSketch_;
  private final ArrayOfItemsSerDe<T> serDe_;
  private boolean isFirstCall_ = true;

  /**
   * Constructs a function given a sketch size and serde
   * @param sketchSize parameter controlling the size of the sketch and the accuracy
   * @param serDe an instance of ArrayOfItemsSerDe to serialize and deserialize arrays of items
   */
  public DataToFrequentItemsSketch(final int sketchSize, final ArrayOfItemsSerDe<T> serDe) {
    super();
    sketchSize_ = sketchSize;
    serDe_ = serDe;
  }

  @Override
  public void accumulate(final Tuple inputTuple) throws IOException {
    if (isFirstCall_) {
      Logger.getLogger(getClass()).info("accumulate was used"); // this is to see in the log which way was used by Pig
      isFirstCall_ = false;
    }
    if (accumSketch_ == null) {
      accumSketch_ = new ItemsSketch<T>(sketchSize_);
    }
    if (inputTuple.size() != 1) throw new IllegalArgumentException("Input tuple must have 1 bag");
    final DataBag bag = (DataBag) inputTuple.get(0);
    updateSketch(bag, accumSketch_);
  }

  @Override
  public void cleanup() {
    accumSketch_ = null;
  }

  @Override
  public Tuple getValue() {
    if (accumSketch_ == null) {
      accumSketch_ = new ItemsSketch<T>(sketchSize_);
    }
    final Tuple outputTuple;
    try {
      outputTuple = Util.serializeSketchToTuple(accumSketch_, serDe_);
    } catch (ExecException ex) {
      throw new RuntimeException("Pig Error: " + ex.getMessage(), ex);
    }
    return outputTuple;
  }

  @Override
  public Tuple exec(final Tuple inputTuple) throws IOException {
    if (isFirstCall_) {
      Logger.getLogger(getClass()).info("exec was used"); // this is to see in the log which way was used by Pig
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

  static <T> void updateSketch(final DataBag bag, final ItemsSketch<T> sketch) throws ExecException {
    if (bag == null) throw new IllegalArgumentException("InputTuple.Field0: Bag may not be null");
    for (final Tuple tuple: bag) {
      if (tuple.size() != 1 && tuple.size() != 2) throw new IllegalArgumentException("Inner tuple of input bag must have 1 or 2 fields.");
      @SuppressWarnings("unchecked")
      final T key = (T) tuple.get(0);
      if (key == null) continue;
      final long value = tuple.size() == 2 ? (long) tuple.get(1) : 1;
      sketch.update(key, value);
    }
  }

}
