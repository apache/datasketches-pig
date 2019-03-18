/*
 * Copyright 2019, Verizon Media.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.pig.cpc;

import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;

import com.yahoo.sketches.cpc.CpcSketch;

public class DataToSketchAlgebraicIntermediate extends AlgebraicIntermediate {

  /**
   * Default constructor for the intermediate pass of an Algebraic function.
   * Assumes default lgK and seed.
   */
  public DataToSketchAlgebraicIntermediate() {
    super(CpcSketch.DEFAULT_LG_K, DEFAULT_UPDATE_SEED);
  }

  /**
   * Constructor for the intermediate pass of an Algebraic function. Pig will call
   * this and pass the same constructor arguments as the base UDF.
   * Assumes default seed.
   *
   * @param lgK in a form of a String
   */
  public DataToSketchAlgebraicIntermediate(final String lgK) {
    super(Integer.parseInt(lgK), DEFAULT_UPDATE_SEED);
  }

  /**
   * Constructor for the intermediate pass of an Algebraic function. Pig will call
   * this and pass the same constructor arguments as the base UDF.
   * 
   * @param lgK parameter controlling the sketch size and accuracy
   * @param seed for the hash function
   */
  public DataToSketchAlgebraicIntermediate(final String lgK, final String seed) {
    super(Integer.parseInt(lgK), Long.parseLong(seed));
  }

  @Override
  boolean isInputRaw() {
    return true;
  }

}
