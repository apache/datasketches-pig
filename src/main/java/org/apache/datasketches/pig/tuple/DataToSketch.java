/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datasketches.pig.tuple;

import static org.apache.datasketches.Util.DEFAULT_NOMINAL_ENTRIES;

import java.io.IOException;

import org.apache.datasketches.tuple.SummaryFactory;
import org.apache.datasketches.tuple.UpdatableSketch;
import org.apache.datasketches.tuple.UpdatableSketchBuilder;
import org.apache.datasketches.tuple.UpdatableSummary;
import org.apache.log4j.Logger;
import org.apache.pig.Accumulator;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;

/**
 * This is a generic implementation to be specialized in concrete UDFs
 * @param <U> Update type
 * @param <S> Summary type
 */
public abstract class DataToSketch<U, S extends UpdatableSummary<U>> extends EvalFunc<Tuple>
    implements Accumulator<Tuple> {

  private final UpdatableSketchBuilder<U, S> sketchBuilder_;
  private UpdatableSketch<U, S> accumSketch_;
  private boolean isFirstCall_ = true;

  /**
   * Constructs a function given a summary factory, default sketch size and default
   * sampling probability of 1.
   * @param summaryFactory an instance of SummaryFactory
   */
  public DataToSketch(final SummaryFactory<S> summaryFactory) {
    this(DEFAULT_NOMINAL_ENTRIES, 1f, summaryFactory);
  }

  /**
   * Constructs a function given a sketch size, summary factory and default
   * sampling probability of 1.
   * @param sketchSize parameter controlling the size of the sketch and the accuracy.
   * It represents nominal number of entries in the sketch. Forced to the nearest power of 2
   * greater than given value.
   * @param summaryFactory an instance of SummaryFactory
   */
  public DataToSketch(final int sketchSize, final SummaryFactory<S> summaryFactory) {
    this(sketchSize, 1f, summaryFactory);
  }

  /**
   * Constructs a function given a sketch size, sampling probability and summary factory
   * @param sketchSize parameter controlling the size of the sketch and the accuracy.
   * It represents nominal number of entries in the sketch. Forced to the nearest power of 2
   * greater than given value.
   * @param samplingProbability parameter from 0 to 1 inclusive
   * @param summaryFactory an instance of SummaryFactory
   */
  public DataToSketch(final int sketchSize, final float samplingProbability,
      final SummaryFactory<S> summaryFactory) {
    super();
    sketchBuilder_ = new UpdatableSketchBuilder<U, S>(summaryFactory)
        .setNominalEntries(sketchSize).setSamplingProbability(samplingProbability);
  }

  @Override
  public void accumulate(final Tuple inputTuple) throws IOException {
    if (isFirstCall_) {
      // this is to see in the log which way was used by Pig
      Logger.getLogger(getClass()).info("accumulate is used");
      isFirstCall_ = false;
    }
    if (accumSketch_ == null) {
      accumSketch_ = sketchBuilder_.build();
    }
    if (inputTuple.size() != 1) {
      throw new IllegalArgumentException("Input tuple must have 1 bag");
    }
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
      accumSketch_ = sketchBuilder_.build();
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

    final UpdatableSketch<U, S> sketch = sketchBuilder_.build();
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
      if (key == null) { continue; }
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
        if (dba.size() != 0) {
          sketch.update(dba.get(), value);
        }
        break;
      case DataType.CHARARRAY:
        final String s = key.toString();
        if (!s.isEmpty()) {
          sketch.update(s, value);
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
