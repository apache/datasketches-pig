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
import org.apache.datasketches.tuple.Summary;
import org.apache.datasketches.tuple.SummaryDeserializer;
import org.apache.datasketches.tuple.SummarySetOperations;
import org.apache.datasketches.tuple.Union;
import org.apache.log4j.Logger;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

/**
 * This is to calculate the intermediate pass (combiner) or the final pass
 * (reducer) of an Algebraic sketch operation. This may be called multiple times
 * (from the mapper and from the reducer). It will receive a bag of values
 * returned by either the Intermediate or the Initial stages, so
 * it needs to be able to differentiate between and interpret both types.
 *
 * @param <S> Type of Summary
 */
public abstract class UnionSketchAlgebraicIntermediateFinal<S extends Summary> extends EvalFunc<Tuple> {
  private final int sketchSize_;
  private final SummarySetOperations<S> summarySetOps_;
  private final SummaryDeserializer<S> summaryDeserializer_;
  private boolean isFirstCall_ = true;

  /**
   * Constructs a function given a summary factory and default sketch size
   * @param summarySetOps an instance of SummarySetOperations
   * @param summaryDeserializer an instance of SummaryDeserializer
   */
  public UnionSketchAlgebraicIntermediateFinal(final SummarySetOperations<S> summarySetOps,
      final SummaryDeserializer<S> summaryDeserializer) {
    this(DEFAULT_NOMINAL_ENTRIES, summarySetOps, summaryDeserializer);
  }

  /**
   * Constructs a function given a sketch size and summary factory
   * @param sketchSize parameter controlling the size of the sketch and the accuracy.
   * It represents nominal number of entries in the sketch. Forced to the nearest power of 2
   * greater than given value.
   * @param summarySetOps an instance of SummarySetOperations
   * @param summaryDeserializer an instance of SummaryDeserializer
   */
  public UnionSketchAlgebraicIntermediateFinal(final int sketchSize,
      final SummarySetOperations<S> summarySetOps, final SummaryDeserializer<S> summaryDeserializer) {
    sketchSize_ = sketchSize;
    summarySetOps_ = summarySetOps;
    summaryDeserializer_ = summaryDeserializer;
  }

  @Override
  public Tuple exec(final Tuple inputTuple) throws IOException {
    if (isFirstCall_) {
      // this is to see in the log which way was used by Pig
      Logger.getLogger(getClass()).info("algebraic is used");
      isFirstCall_ = false;
    }
    final Union<S> union = new Union<S>(sketchSize_, summarySetOps_);

    final DataBag bag = (DataBag) inputTuple.get(0);
    if (bag == null) {
      throw new IllegalArgumentException("InputTuple.Field0: Bag may not be null");
    }

    for (final Tuple dataTuple: bag) {
      final Object item = dataTuple.get(0);
      if (item instanceof DataBag) {
        // this is from a prior call to the initial function, so there is a nested bag.
        for (Tuple innerTuple: (DataBag) item) {
          final Sketch<S> incomingSketch = Util.deserializeSketchFromTuple(innerTuple, summaryDeserializer_);
          union.update(incomingSketch);
        }
      } else if (item instanceof DataByteArray) {
        // This is a sketch from a call to the Intermediate function
        // Add it to the current union.
        final Sketch<S> incomingSketch = Util.deserializeSketchFromTuple(dataTuple, summaryDeserializer_);
        union.update(incomingSketch);
      } else {
        // we should never get here.
        throw new IllegalArgumentException(
            "InputTuple.Field0: Bag contains unrecognized types: " + item.getClass().getName());
      }
    }
    return Util.tupleFactory.newTuple(new DataByteArray(union.getResult().toByteArray()));
  }
}
