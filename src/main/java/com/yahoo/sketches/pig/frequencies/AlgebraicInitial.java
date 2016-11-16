/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.pig.frequencies;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

/**
 * This is a common pass-through implementation for initial step of an Algebraic operation
 */
public abstract class AlgebraicInitial extends EvalFunc<Tuple> {
  @Override
  public Tuple exec(final Tuple inputTuple) throws IOException {
    final DataBag bag = (DataBag) inputTuple.get(0);
    if (bag == null) {
      throw new IllegalArgumentException("InputTuple.Field0: Bag may not be null");
    }
    return inputTuple;
  }
}
