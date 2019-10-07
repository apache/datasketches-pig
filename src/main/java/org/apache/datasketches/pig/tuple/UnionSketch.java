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

import org.apache.datasketches.tuple.Sketch;
import org.apache.datasketches.tuple.Sketches;
import org.apache.datasketches.tuple.Summary;
import org.apache.datasketches.tuple.SummaryDeserializer;
import org.apache.datasketches.tuple.SummarySetOperations;
import org.apache.datasketches.tuple.Union;
import org.apache.log4j.Logger;
import org.apache.pig.Accumulator;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

/**
 * This is a generic implementation to be specialized in concrete UDFs
 * @param <S> Summary type
 */
public abstract class UnionSketch<S extends Summary> extends EvalFunc<Tuple> implements Accumulator<Tuple> {
  private final int sketchSize_;
  private final SummarySetOperations<S> summarySetOps_;
  private final SummaryDeserializer<S> summaryDeserializer_;
  private Union<S> union_;
  private boolean isFirstCall_ = true;

  /**
   * Constructs a function given a summary set operations, summary deserializer and default sketch size
   * @param summarySetOps an instance of SummarySetOperations
   * @param summaryDeserializer an instance of SummaryDeserializer
   */
  public UnionSketch(final SummarySetOperations<S> summarySetOps,
      final SummaryDeserializer<S> summaryDeserializer) {
    this(DEFAULT_NOMINAL_ENTRIES, summarySetOps, summaryDeserializer);
  }

  /**
   * Constructs a function given a sketch size, summary set operations and summary deserializer
   * @param sketchSize parameter controlling the size of the sketch and the accuracy.
   * It represents nominal number of entries in the sketch. Forced to the nearest power of 2
   * greater than given value.
   * @param summarySetOps an instance of SummarySetOperations
   * @param summaryDeserializer an instance of SummaryDeserializer
   */
  public UnionSketch(final int sketchSize, final SummarySetOperations<S> summarySetOps,
      final SummaryDeserializer<S> summaryDeserializer) {
    super();
    sketchSize_ = sketchSize;
    summarySetOps_ = summarySetOps;
    summaryDeserializer_ = summaryDeserializer;
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
    final DataBag bag = (DataBag) inputTuple.get(0);
    final Union<S> union = new Union<S>(sketchSize_, summarySetOps_);
    updateUnion(bag, union, summaryDeserializer_);
    return Util.tupleFactory.newTuple(new DataByteArray(union.getResult().toByteArray()));
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
    final DataBag bag = (DataBag) inputTuple.get(0);
    if (bag == null || bag.size() == 0) { return; }
    if (union_ == null) {
      union_ = new Union<S>(sketchSize_, summarySetOps_);
    }
    updateUnion(bag, union_, summaryDeserializer_);
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
    if (union_ != null) { union_.reset(); }
  }

  private static <S extends Summary> void updateUnion(final DataBag bag, final Union<S> union,
      final SummaryDeserializer<S> summaryDeserializer) throws ExecException {
    for (final Tuple innerTuple: bag) {
      if ((innerTuple.size() != 1) || (innerTuple.get(0) == null)) {
        continue;
      }
      final Sketch<S> incomingSketch = Util.deserializeSketchFromTuple(innerTuple, summaryDeserializer);
      union.update(incomingSketch);
    }
  }

}
