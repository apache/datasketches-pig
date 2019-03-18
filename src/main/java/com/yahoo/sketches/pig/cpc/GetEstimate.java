/*
 * Copyright 2019, Verizon Media.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.pig.cpc;

import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

import com.yahoo.sketches.cpc.CpcSketch;

/**
 * This is a User Defined Function (UDF) for getting a distinct count estimate from a given CpcdSketch
 *
 * @author Alexander Saydakov
 */
public class GetEstimate extends EvalFunc<Double> {

  private final long seed_;

  /**
   * Constructor with default seed
   */
  public GetEstimate() {
    this(DEFAULT_UPDATE_SEED);
  }

  /**
   * Constructor with given seed
   * @param seed in a form of a String
   */
  public GetEstimate(final String seed) {
    this(Long.parseLong(seed));
  }

  /**
   * Base constructor
   * @param seed parameter for the hash function
   */
  GetEstimate(final long seed) {
    seed_ = seed;
  }

  @Override
  public Double exec(final Tuple sketchTuple) throws IOException {
    if ((sketchTuple == null) || (sketchTuple.size() == 0)) {
      return null;
    }
    final DataByteArray dba = (DataByteArray) sketchTuple.get(0);
    final CpcSketch sketch = CpcSketch.heapify(dba.get(), seed_);
    return sketch.getEstimate();
  }

}
