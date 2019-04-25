/*
 * Copyright 2019, Verizon Media.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.pig.cpc;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.yahoo.sketches.cpc.CpcSketch;

/**
 * Class used to calculate the intermediate combiner pass of an <i>Algebraic</i> sketch
 * operation. This is called from the combiner, and may be called multiple times (from a mapper
 * and from a reducer). It will receive a bag of values returned by either <i>Intermediate</i>
 * or <i>Initial</i> stages, so it needs to be able to differentiate between and
 * interpret both types.
 *
 * @author Alexander Saydakov
 */
abstract class AlgebraicIntermediate extends EvalFunc<Tuple> {

  private static final TupleFactory TUPLE_FACTORY = TupleFactory.getInstance();

  private final int lgK_;
  private final long seed_;
  private Tuple emptySketchTuple_; // this is to cash an empty sketch tuple
  private boolean isFirstCall_ = true; // for logging

  /**
   * Constructor with primitives for the intermediate pass of an Algebraic function.
   *
   * @param lgK parameter controlling the sketch size and accuracy
   * @param seed the given seed
   */
  public AlgebraicIntermediate(final int lgK, final long seed) {
    lgK_ = lgK;
    seed_ = seed;
  }

  @Override
  public Tuple exec(final Tuple inputTuple) throws IOException {
    if (isFirstCall_) {
      Logger.getLogger(getClass()).info("Algebraic was used");
      isFirstCall_ = false;
    }
    final DataByteArray dba = AlgebraicFinal.process(inputTuple, lgK_, seed_, isInputRaw());
    if (dba == null) {
      return getEmptySketchTuple();
    }
    return TUPLE_FACTORY.newTuple(dba);
  }

  abstract boolean isInputRaw();

  private Tuple getEmptySketchTuple() {
    if (emptySketchTuple_ == null) {
      emptySketchTuple_ = TUPLE_FACTORY.newTuple(new DataByteArray(
          new CpcSketch(lgK_, seed_).toByteArray()));
    }
    return emptySketchTuple_;
  }

}
