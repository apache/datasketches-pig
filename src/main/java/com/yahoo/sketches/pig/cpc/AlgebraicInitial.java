/*
 * Copyright 2019, Verizon Media.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.pig.cpc;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

/**
 * Class used to calculate the initial pass of an Algebraic sketch operation.
 *
 * <p>The Initial class simply passes through all records unchanged so that they can be
 * processed by the intermediate processor instead.
 * 
 * @author Alexander Saydakov
 */
public class AlgebraicInitial extends EvalFunc<Tuple> {

  private boolean isFirstCall_ = true;

  /**
   * Default constructor for the initial pass of an Algebraic function.
   */
  public AlgebraicInitial() {}

  /**
   * Constructor for the initial pass of an Algebraic function. Pig will call this and pass the
   * same constructor arguments as the original UDF. In this case the arguments are ignored.
   *
   * @param lgK in a form of a String
   */
  public AlgebraicInitial(final String lgK) {}

  /**
   * Constructor for the initial pass of an Algebraic function. Pig will call this and pass the
   * same constructor arguments as the original UDF. In this case the arguments are ignored.
   *
   * @param lgK in a form of a String
   * @param seed in a form of a String
   */
  public AlgebraicInitial(final String lgK, final String seed) {}

  @Override
  public Tuple exec(final Tuple inputTuple) throws IOException {
    if (isFirstCall_) {
      Logger.getLogger(getClass()).info("Algebraic was used");
      isFirstCall_ = false;
    }
    return inputTuple;
  }

}
