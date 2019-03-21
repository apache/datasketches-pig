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
 * This is a User Defined Function (UDF) for printing a human-readable summary of a given CpcSketch
 * @author Alexander Saydakov
 */
public class SketchToString extends EvalFunc<String> {

  private final boolean isDetailed_;
  private final long seed_;

  /**
   * Prints only the sketch summary using the default seed.
   */
  public SketchToString() {
    this(false, DEFAULT_UPDATE_SEED);
  }

  /**
   * Prints the summary and details using the default seed.
   *
   * @param isDetailed flag to print the sketch detail
   */
  public SketchToString(final String isDetailed) {
    this(Boolean.parseBoolean(isDetailed), DEFAULT_UPDATE_SEED);
  }

  /**
   * Prints the summary and details using a given seed.
   *
   * @param isDetailed flag to print the sketch detail
   * @param seed parameter for the hash function
   */
  public SketchToString(final String isDetailed, final String seed) {
    this(Boolean.parseBoolean(isDetailed), Long.parseLong(seed));
  }

  /**
   * Internal constructor with primitive parameters.
   *
   * @param isDetailed flag to print the sketch detail
   * @param seed parameter for the hash function
   */
  private SketchToString(final boolean isDetailed, final long seed) {
    isDetailed_ = isDetailed;
    seed_ = seed;
  }

  @Override
  public String exec(final Tuple sketchTuple) throws IOException {
    if ((sketchTuple == null) || (sketchTuple.size() == 0)) {
      return null;
    }
    final DataByteArray dba = (DataByteArray) sketchTuple.get(0);
    final CpcSketch sketch = CpcSketch.heapify(dba.get(), seed_);
    return sketch.toString(isDetailed_);
  }

}
