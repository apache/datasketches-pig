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
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;

import com.yahoo.sketches.tuple.UpdatableSketch;
import com.yahoo.sketches.tuple.UpdatableSummary;
import com.yahoo.sketches.tuple.SummaryFactory;
import com.yahoo.sketches.tuple.UpdatableSketchBuilder;

/**
 * This is a generic implementation to be specialized in concrete UDFs 
 * @param <U> Update type
 * @param <S> Summary type
 */
abstract class DataToSketch<U, S extends UpdatableSummary<U>> extends EvalFunc<Tuple> implements Accumulator<Tuple> {
  private final int sketchSize_;
  private float samplingProbability_;
  private SummaryFactory<S> summaryFactory_;
  private UpdatableSketch<U, S> accumSketch_;
  private boolean isFirstCall_ = true;

  DataToSketch(int sketchSize, SummaryFactory<S> summaryFactory) {
    this(sketchSize, 1f, summaryFactory);
  }

  DataToSketch(int sketchSize, float samplingProbability, SummaryFactory<S> summaryFactory) {
    super();
    this.sketchSize_ = sketchSize;
    this.samplingProbability_ = samplingProbability; 
    this.summaryFactory_ = summaryFactory;
  }

  @Override
  public void accumulate(Tuple inputTuple) throws IOException {
    if (isFirstCall_) {
      Logger.getLogger(getClass()).info("accumulate is used"); // this is to see in the log which way was used by Pig
      isFirstCall_ = false;
    }
    if (accumSketch_ == null) {
      accumSketch_ = new UpdatableSketchBuilder<U, S>(summaryFactory_).setNominalEntries(sketchSize_).setSamplingProbability(samplingProbability_).build();
    }
    if (inputTuple.size() != 1) throw new IllegalArgumentException("Input tuple must have 1 bag");
    DataBag bag = (DataBag) inputTuple.get(0);
    updateSketch(bag, accumSketch_);
  }

  @Override
  public void cleanup() {
    accumSketch_ = null;
  }

  @Override
  public Tuple getValue() {
    if (accumSketch_ == null) {
      accumSketch_ = new UpdatableSketchBuilder<U, S>(summaryFactory_).setNominalEntries(sketchSize_).setSamplingProbability(samplingProbability_).build();
    }
    Tuple outputTuple;
    try {
      outputTuple = SerializerDeserializer.serializeSketchToTuple(accumSketch_.compact());
    } catch (ExecException ex) {
      throw new RuntimeException("Pig Error: " + ex.getMessage(), ex);
    }
    return outputTuple;
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

  static <U, S extends UpdatableSummary<U>> void updateSketch(DataBag bag, UpdatableSketch<U, S> sketch) throws ExecException {
    if (bag == null) throw new IllegalArgumentException("InputTuple.Field0: Bag may not be null");
    for (Tuple tuple: bag) {
      if (tuple.size() != 2) throw new IllegalArgumentException("Inner tuple of input bag must have 2 fields.");

      Object key = tuple.get(0);
      if (key == null) continue;
      @SuppressWarnings("unchecked")
      U value = (U) tuple.get(1);

      switch (tuple.getType(0)) {
      case DataType.BYTE:
        sketch.update(((Byte) key).longValue(), value);
        break;
      case DataType.INTEGER:
        sketch.update(((Integer) key).longValue(), value);
        break;
      case DataType.LONG:
        sketch.update((Long) key, value);
        break;
      case DataType.FLOAT:
        sketch.update((Float) key, value);
        break;
      case DataType.DOUBLE:
        sketch.update((Double) key, value);
        break;
      case DataType.BYTEARRAY:
        DataByteArray dba = (DataByteArray) key;
        if (dba.size() != 0) sketch.update(dba.get(), value);
        break;
      case DataType.CHARARRAY:
        String s = key.toString();
        if (!s.isEmpty()) sketch.update(s, value);
        break;
      default:
        throw new IllegalArgumentException("Field 0 must be one of "
            + "NULL, BYTE, INTEGER, LONG, FLOAT, DOUBLE, BYTEARRAY or CHARARRAY. " + "Type = "
            + DataType.findTypeName(tuple.getType(0)) + ", Object = " + key.toString());
      }
    }
  }
}