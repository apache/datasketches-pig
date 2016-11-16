/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.pig.tuple;

import static com.yahoo.sketches.Util.DEFAULT_NOMINAL_ENTRIES;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.pig.Accumulator;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;

import com.yahoo.sketches.tuple.ArrayOfDoublesUpdatableSketch;
import com.yahoo.sketches.tuple.ArrayOfDoublesUpdatableSketchBuilder;

abstract class DataToArrayOfDoublesSketchBase extends EvalFunc<Tuple> implements Accumulator<Tuple> {
  private final int sketchSize_;
  private final float samplingProbability_;
  private final int numValues_;
  private ArrayOfDoublesUpdatableSketch accumSketch_;
  private boolean isFirstCall_ = true;

  DataToArrayOfDoublesSketchBase() {
    this(DEFAULT_NOMINAL_ENTRIES, 1f, 1);
  }

  DataToArrayOfDoublesSketchBase(final int numValues) {
    this(DEFAULT_NOMINAL_ENTRIES, 1f, numValues);
  }

  DataToArrayOfDoublesSketchBase(final int sketchSize, final int numValues) {
    this(sketchSize, 1f, numValues);
  }

  DataToArrayOfDoublesSketchBase(final int sketchSize, final float samplingProbability, final int numValues) {
    super();
    sketchSize_ = sketchSize;
    samplingProbability_ = samplingProbability;
    numValues_ = numValues;
  }

  @Override
  public void accumulate(final Tuple inputTuple) throws IOException {
    if (isFirstCall_) {
      // this is to see in the log which way was used by Pig
      Logger.getLogger(getClass()).info("accumulate is used");
      isFirstCall_ = false;
    }
    if (accumSketch_ == null) {
      accumSketch_ =
          new ArrayOfDoublesUpdatableSketchBuilder().setNominalEntries(sketchSize_)
            .setSamplingProbability(samplingProbability_).setNumberOfValues(numValues_).build();
    }
    if (inputTuple.size() != 1) {
      throw new IllegalArgumentException("Input tuple must have 1 bag");
    }
    final DataBag bag = (DataBag) inputTuple.get(0);
    updateSketch(bag, accumSketch_, numValues_);
  }

  @Override
  public void cleanup() {
    accumSketch_ = null;
  }

  @Override
  public Tuple getValue() {
    if (accumSketch_ == null) {
      accumSketch_ =
          new ArrayOfDoublesUpdatableSketchBuilder().setNominalEntries(sketchSize_)
            .setSamplingProbability(samplingProbability_).setNumberOfValues(numValues_).build();
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
    if (inputTuple.size() != 1) {
      throw new IllegalArgumentException("Input tuple must have 1 bag");
    }

    final ArrayOfDoublesUpdatableSketch sketch =
        new ArrayOfDoublesUpdatableSketchBuilder().setNominalEntries(sketchSize_)
          .setSamplingProbability(samplingProbability_).setNumberOfValues(numValues_).build();
    final DataBag bag = (DataBag) inputTuple.get(0);
    updateSketch(bag, sketch, numValues_);
    return Util.tupleFactory.newTuple(new DataByteArray(sketch.compact().toByteArray()));
  }

  static void updateSketch(
      final DataBag bag, final ArrayOfDoublesUpdatableSketch sketch, final int numValues)
          throws ExecException {
    if (bag == null) {
      throw new IllegalArgumentException("InputTuple.Field0: Bag may not be null");
    }
    final double[] values = new double[numValues];
    for (final Tuple tuple: bag) {
      if (tuple.size() != numValues + 1) {
        throw new IllegalArgumentException("Inner tuple of input bag must have " + (numValues + 1)
            + " fields.");
      }

      final Object key = tuple.get(0);
      if (key == null) { continue; }
      for (int i = 0; i < numValues; i++) {
        values[i] = (Double) tuple.get(i + 1);
      }

      switch (tuple.getType(0)) {
      case DataType.BYTE:
        sketch.update(((Byte) key).longValue(), values);
        break;
      case DataType.INTEGER:
        sketch.update(((Integer) key).longValue(), values);
        break;
      case DataType.LONG:
        sketch.update((Long) key, values);
        break;
      case DataType.FLOAT:
        sketch.update((Float) key, values);
        break;
      case DataType.DOUBLE:
        sketch.update((Double) key, values);
        break;
      case DataType.BYTEARRAY:
        final DataByteArray dba = (DataByteArray) key;
        if (dba.size() != 0) {
          sketch.update(dba.get(), values);
        }
        break;
      case DataType.CHARARRAY:
        final String s = key.toString();
        if (!s.isEmpty()) {
          sketch.update(s, values);
        }
        break;
      default:
        throw new IllegalArgumentException("Field 0 must be one of "
            + "NULL, BYTE, INTEGER, LONG, FLOAT, DOUBLE, BYTEARRAY or CHARARRAY. " + "Type = "
            + DataType.findTypeName(tuple.getType(0)) + ", Object = " + key.toString());
      }
    }
  }
}
