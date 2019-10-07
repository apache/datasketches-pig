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
import org.apache.datasketches.tuple.SummaryDeserializer;
import org.apache.datasketches.tuple.SummaryFactory;
import org.apache.datasketches.tuple.SummarySetOperations;
import org.apache.datasketches.tuple.Union;
import org.apache.datasketches.tuple.UpdatableSketch;
import org.apache.datasketches.tuple.UpdatableSketchBuilder;
import org.apache.datasketches.tuple.UpdatableSummary;
import org.apache.log4j.Logger;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

/**
 * Class used to calculate the intermediate pass (combiner) or the final pass
 * (reducer) of an Algebraic sketch operation. This may be called multiple times
 * (from the mapper and from the reducer). It will receive a bag of values
 * returned by either the Intermediate stage or the Initial stages, so
 * it needs to be able to differentiate between and interpret both types.
 * @param <U> Type of the update value
 * @param <S> Type of the summary
 */
public abstract class DataToSketchAlgebraicIntermediateFinal<U, S extends UpdatableSummary<U>>
    extends EvalFunc<Tuple> {
  private final int sketchSize_;
  private final SummarySetOperations<S> summarySetOps_;
  private final SummaryDeserializer<S> summaryDeserializer_;
  private final UpdatableSketchBuilder<U, S> sketchBuilder_;
  private boolean isFirstCall_ = true;

  /**
   * Constructs a function given a summary factory, summary set operations, summary deserializer,
   * default sketch size and default sampling probability of 1.
   * @param summaryFactory an instance of SummaryFactory
   * @param summarySetOps an instance of SummarySetOperaions
   * @param summaryDeserializer an instance of SummaryDeserializer
   */
  public DataToSketchAlgebraicIntermediateFinal(final SummaryFactory<S> summaryFactory,
      final SummarySetOperations<S> summarySetOps, final SummaryDeserializer<S> summaryDeserializer) {
    this(DEFAULT_NOMINAL_ENTRIES, 1f, summaryFactory, summarySetOps, summaryDeserializer);
  }

  /**
   * Constructs a function given a sketch size, summary factory, summary set operations,
   * summary deserializer and default sampling probability of 1.
   * @param sketchSize parameter controlling the size of the sketch and the accuracy.
   * It represents nominal number of entries in the sketch. Forced to the nearest power of 2
   * greater than given value.
   * @param summaryFactory an instance of SummaryFactory
   * @param summarySetOps an instance of SummarySetOperaions
   * @param summaryDeserializer an instance of SummaryDeserializer
   */
  public DataToSketchAlgebraicIntermediateFinal(final int sketchSize,
      final SummaryFactory<S> summaryFactory, final SummarySetOperations<S> summarySetOps,
      final SummaryDeserializer<S> summaryDeserializer) {
    this(sketchSize, 1f, summaryFactory, summarySetOps, summaryDeserializer);
  }

  /**
   * Constructs a function given a sketch size, sampling probability, summary factory,
   * summary set operations and summary deserializer
   * @param sketchSize parameter controlling the size of the sketch and the accuracy.
   * It represents nominal number of entries in the sketch. Forced to the nearest power of 2
   * greater than given value.
   * @param samplingProbability parameter from 0 to 1 inclusive
   * @param summaryFactory an instance of SummaryFactory
   * @param summarySetOps an instance of SummarySetOperaions
   * @param summaryDeserializer an instance of SummaryDeserializer
   */
  public DataToSketchAlgebraicIntermediateFinal(final int sketchSize, final float samplingProbability,
      final SummaryFactory<S> summaryFactory, final SummarySetOperations<S> summarySetOps,
      final SummaryDeserializer<S> summaryDeserializer) {
    sketchSize_ = sketchSize;
    summarySetOps_ = summarySetOps;
    summaryDeserializer_ = summaryDeserializer;
    sketchBuilder_ = new UpdatableSketchBuilder<U, S>(summaryFactory)
        .setNominalEntries(sketchSize).setSamplingProbability(samplingProbability);
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
        // this is a bag from the Initial function.
        // just insert each item of the tuple into the sketch
        final UpdatableSketch<U, S> sketch = sketchBuilder_.build();
        DataToSketch.updateSketch((DataBag) item, sketch);
        union.update(sketch);
      } else if (item instanceof DataByteArray) {
        // This is a sketch from a prior call to the
        // Intermediate function. merge it with the
        // current sketch.
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
