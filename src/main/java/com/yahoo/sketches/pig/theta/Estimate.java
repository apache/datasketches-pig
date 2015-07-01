/*
 * Copyright 2015, Yahoo! Inc.
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
 * Returns the unique count estimate of a sketch as a Double.
 * 
 * @author LeeRhodes 
 */
public class Estimate extends EvalFunc<Double> {
  private final long seed_;
  
  /**
   * Constructs with the DEFAULT_UPDATE_SEED used when deserializing the sketch. 
   */
  public Estimate() {
    this(Util.DEFAULT_UPDATE_SEED);
  }
  
  /**
   * Constructs with the given seed.
   * @param seedStr the string seed used when deserializing the sketch.
   */
  public Estimate(String seedStr) {
    this(Long.parseLong(seedStr));
  }
  
  /**
   * Constructs with the given seed.
   * @param seed used when deserializing the sketch.
   */
  public Estimate(long seed) {
    super();
    seed_ = seed;
  }
  
  @Override
  public Double exec(Tuple sketchTuple) throws IOException { //throws is in API
    if ((sketchTuple == null) || (sketchTuple.size() == 0)) {
      return null;
    }
    Sketch sketch =  tupleToSketch(sketchTuple, seed_);
    return sketch.getEstimate();
  }
}
