/*
 * Copyright 2017, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.pig.hll;

import static com.yahoo.sketches.pig.hll.DataToSketch.DEFAULT_HLL_TYPE;
import static com.yahoo.sketches.pig.hll.DataToSketch.DEFAULT_LG_K;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;

import com.yahoo.sketches.hll.TgtHllType;
import com.yahoo.sketches.hll.Union;

public class DataToSketchAlgebraicFinal extends AlgebraicFinal {

  /**
   * Default constructor for the final pass of an Algebraic function.
   * Assumes default lgK and target HLL type.
   */
  public DataToSketchAlgebraicFinal() {
    super(DEFAULT_LG_K, DEFAULT_HLL_TYPE);
  }

  /**
   * Constructor for the final pass of an Algebraic function. Pig will call
   * this and pass the same constructor arguments as the base UDF.
   * Assumes default HLL target type.
   *
   * @param lgK in a form of a String
   */
  public DataToSketchAlgebraicFinal(final String lgK) {
    super(Integer.parseInt(lgK), DEFAULT_HLL_TYPE);
  }

  /**
   * Constructor for the final pass of an Algebraic function. Pig will call
   * this and pass the same constructor arguments as the base UDF.
   * 
   * @param lgK parameter controlling the sketch size and accuracy
   * @param tgtHllType HLL type of the resulting sketch
   */
  public DataToSketchAlgebraicFinal(final String lgK, final String tgtHllType) {
    super(Integer.parseInt(lgK), TgtHllType.valueOf(tgtHllType));
  }

  @Override
  void updateUnion(final DataBag bag, final Union union) throws ExecException {
    DataToSketch.updateUnion(bag, union);
  }

}
