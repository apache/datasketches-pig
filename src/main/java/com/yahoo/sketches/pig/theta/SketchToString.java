/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.pig.theta;

import static com.yahoo.sketches.pig.theta.PigUtil.tupleToSketch;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import com.yahoo.sketches.Util;
import com.yahoo.sketches.theta.Sketch;

/**
 * This is a User Defined Function (UDF) for "pretty printing" the summary of a sketch
 * from a Sketch Tuple.
 *
 * <p>
 * Refer to {@link DataToSketch#exec(Tuple)} for the definition of a Sketch Tuple.
 * </p>
 * @author Lee Rhodes
 */
public class SketchToString extends EvalFunc<String> {
  private boolean detailOut = false;
  private final long seed_;

  /**
   * Pretty prints only the sketch summary.
   */
  public SketchToString() {
    this(false, Util.DEFAULT_UPDATE_SEED);
  }

  /**
   * Pretty prints all bucket detail plus the sketch summary based on outputDetail.
   *
   * @param outputDetailStr If the first character is a "T" or "t" the output will include the bucket
   * detail. Otherwise only the sketch summary will be output.
   */
  public SketchToString(final String outputDetailStr) {
    this( outputDetailStr.substring(0, 1).equalsIgnoreCase("T"), Util.DEFAULT_UPDATE_SEED);
  }

  /**
   * Pretty prints all bucket detail plus the sketch summary based on outputDetail.
   *
   * @param outputDetailStr If the first character is a "T" or "t" the output will include the bucket
   * detail. Otherwise only the sketch summary will be output.
   * @param seedStr the seed string
   */
  public SketchToString(final String outputDetailStr, final String seedStr) {
    this( outputDetailStr.substring(0, 1).equalsIgnoreCase("T"), Long.parseLong(seedStr));
  }

  /**
   * Pretty prints all bucket detail plus the sketch summary based on outputDetail.
   *
   * @param outputDetail If the first character is a "T" or "t" the output will include the bucket
   * detail. Otherwise only the sketch summary will be output.
   * @param seed the seed string
   */
  public SketchToString(final boolean outputDetail, final long seed) {
    super();
    detailOut = outputDetail;
    seed_ = seed;
  }

  @Override
  public String exec(final Tuple sketchTuple) throws IOException { //throws is in API
    if ((sketchTuple == null) || (sketchTuple.size() == 0)) {
      return null;
    }
    final Sketch sketch = tupleToSketch(sketchTuple, seed_);
    return sketch.toString(true, detailOut, 8, true);
  }

}
