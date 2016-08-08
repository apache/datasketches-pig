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

import com.yahoo.sketches.tuple.SummaryFactory;
import com.yahoo.sketches.tuple.UpdatableSketch;
import com.yahoo.sketches.tuple.UpdatableSketchBuilder;
import com.yahoo.sketches.tuple.UpdatableSummary;

/**
 * This is a generic implementation to be specialized in concrete UDFs 
 * @param <U> Update type
 * @param <S> Summary type
 */
public abstract class DataToSketch<U, S extends UpdatableSummary<U>> extends EvalFunc<Tuple> 
    implements Accumulator<Tuple> {
  private final int sketchSize_;
  private float samplingProbability_;
  private SummaryFactory<S> summaryFactory_;
  private UpdatableSketch<U, S> accumSketch_;
  private boolean isFirstCall_ = true;

  public DataToSketch(final int sketchSize, final SummaryFactory<S> summaryFactory) {
    this(sketchSize, 1f, summaryFactory);
  }

  public DataToSketch(final int sketchSize, final float samplingProbability, 
      final SummaryFactory<S> summaryFactory) {
    super();
    this.sketchSize_ = sketchSize;
    this.samplingProbability_ = samplingProbability; 
    this.summaryFactory_ = summaryFactory;
  }

  @Override
  public void accumulate(final Tuple inputTuple) throws IOException {
    if (isFirstCall_) {
      Logger.getLogger(getClass()).info("accumulate is used"); // this is to see in the log which way was used by Pig
      isFirstCall_ = false;
    }
    if (accumSketch_ == null) {
      accumSketch_ = new UpdatableSketchBuilder<U, S>(summaryFactory_)
          .setNominalEntries(sketchSize_).setSamplingProbability(samplingProbability_).build();
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
      accumSketch_ = new UpdatableSketchBuilder<U, S>(summaryFactory_)
          .setNominalEntries(sketchSize_).setSamplingProbability(samplingProbability_).build();
    }
    return Util.tupleFactory.newTuple(new DataByteArray(accumSketch_.compact().toByteArray()));
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
    if (inputTuple.size() != 1) throw new IllegalArgumentException("Input tuple must have 1 bag");

    final UpdatableSketch<U, S> sketch = new UpdatableSketchBuilder<U, S>(summaryFactory_)
        .setNominalEntries(sketchSize_).setSamplingProbability(samplingProbability_).build();
    final DataBag bag = (DataBag) inputTuple.get(0);
    updateSketch(bag, sketch);
    return Util.tupleFactory.newTuple(new DataByteArray(sketch.compact().toByteArray()));
  }

  static <U, S extends UpdatableSummary<U>> void updateSketch(final DataBag bag, 
        final UpdatableSketch<U, S> sketch) throws ExecException {
    if (bag == null) {
      throw new IllegalArgumentException("InputTuple.Field0: Bag may not be null");
    }
    for (final Tuple tuple: bag) {
      if (tuple.size() != 2) {
        throw new IllegalArgumentException("Inner tuple of input bag must have 2 fields.");
      }

      final Object key = tuple.get(0);
      if (key == null) continue;
      @SuppressWarnings("unchecked")
      final U value = (U) tuple.get(1);

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
        final DataByteArray dba = (DataByteArray) key;
        if (dba.size() != 0) sketch.update(dba.get(), value);
        break;
      case DataType.CHARARRAY:
        final String s = key.toString();
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