/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.pig.tuple;

import static com.yahoo.sketches.Util.DEFAULT_NOMINAL_ENTRIES;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

import com.yahoo.sketches.tuple.Sketch;
import com.yahoo.sketches.tuple.SummaryFactory;
import com.yahoo.sketches.tuple.Union;
import com.yahoo.sketches.tuple.UpdatableSketch;
import com.yahoo.sketches.tuple.UpdatableSketchBuilder;
import com.yahoo.sketches.tuple.UpdatableSummary;

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
  private final SummaryFactory<S> summaryFactory_;
  private final UpdatableSketchBuilder<U, S> sketchBuilder_;
  private boolean isFirstCall_ = true;

  /**
   * Constructs a function given a summary factory, default sketch size and default
   * sampling probability of 1.
   * @param summaryFactory an instance of SummaryFactory
   */
  public DataToSketchAlgebraicIntermediateFinal(final SummaryFactory<S> summaryFactory) {
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
  public DataToSketchAlgebraicIntermediateFinal(final int sketchSize,
      final SummaryFactory<S> summaryFactory) {
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
  public DataToSketchAlgebraicIntermediateFinal(final int sketchSize,
      final float samplingProbability, final SummaryFactory<S> summaryFactory) {
    sketchSize_ = sketchSize;
    summaryFactory_ = summaryFactory;
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
    final Union<S> union = new Union<S>(sketchSize_, summaryFactory_);

    final DataBag bag = (DataBag) inputTuple.get(0);
    if (bag == null) {
      throw new IllegalArgumentException("InputTuple.Field0: Bag may not be null");
    }

    for (final Tuple dataTuple: bag) {
      final Object item = dataTuple.get(0);
      if (item instanceof DataBag) {
        // this is a bag from the Initial function.
        // just insert each item of the tuple into the sketch
        UpdatableSketch<U, S> sketch = sketchBuilder_.build();
        DataToSketch.updateSketch((DataBag) item, sketch);
        union.update(sketch);
      } else if (item instanceof DataByteArray) {
        // This is a sketch from a prior call to the
        // Intermediate function. merge it with the
        // current sketch.
        final Sketch<S> incomingSketch = Util.deserializeSketchFromTuple(dataTuple);
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
