/*
 * Copyright 2017, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.pig.hll;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.hll.HllSketch;

/**
 * This is a User Defined Function (UDF) for "pretty printing" the summary of an HllSketch
 *
 * @author Alexander Saydakov
 */
public class SketchToString extends EvalFunc<String> {

  private final boolean hllDetail_;
  private final boolean auxDetail_;

  /**
   * Prints only the sketch summary.
   */
  public SketchToString() {
    this(false, false);
  }

  /**
   * Prints the summary and details based on given input parameters.
   *
   * @param hllDetail flag to print the HLL sketch detail
   * @param auxDetail flag to print the auxiliary detail
   */
  public SketchToString(final String hllDetail, final String auxDetail) {
    this(Boolean.parseBoolean(hllDetail), Boolean.parseBoolean(auxDetail));
  }

  /**
   * Internal constructor with primitive parameters.
   *
   * @param hllDetail flag to print the HLL sketch detail
   * @param auxDetail flag to print the auxiliary detail
   */
  private SketchToString(final boolean hllDetail, final boolean auxDetail) {
    super();
    hllDetail_ = hllDetail;
    auxDetail_ = auxDetail;
  }

  @Override
  public String exec(final Tuple sketchTuple) throws IOException {
    if ((sketchTuple == null) || (sketchTuple.size() == 0)) {
      return null;
    }
    final DataByteArray dba = (DataByteArray) sketchTuple.get(0);
    final HllSketch sketch = HllSketch.wrap(Memory.wrap(dba.get()));
    return sketch.toString(true, hllDetail_, auxDetail_);
  }

}
