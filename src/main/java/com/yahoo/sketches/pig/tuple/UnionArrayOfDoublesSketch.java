/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.pig.tuple;

import org.apache.pig.Algebraic;

/**
 * This is to union ArrayOfDoublesSketches.
 * It supports all three ways: exec(), Accumulator and Algebraic
 */
public class UnionArrayOfDoublesSketch extends UnionArrayOfDoublesSketchBase implements Algebraic {
  /**
   * Constructor with default sketch size and default number of values of 1.
   */
  public UnionArrayOfDoublesSketch() {
    super();
  }

  /**
   * Constructor with default sketch size and given number of values.
   * @param numValues String representation of number of values per key
   */
  public UnionArrayOfDoublesSketch(final String numValues) {
    super(Integer.parseInt(numValues));
  }

  /**
   * Constructor with given sketch size and vumber of values.
   * @param sketchSize String representation of sketch size
   * @param numValues String representation of number of values per key
   */
  public UnionArrayOfDoublesSketch(final String sketchSize, final String numValues) {
    super(Integer.parseInt(sketchSize), Integer.parseInt(numValues));
  }

  @Override
  public String getInitial() {
    return Initial.class.getName();
  }

  @Override
  public String getIntermed() {
    return IntermediateFinal.class.getName();
  }

  @Override
  public String getFinal() {
    return IntermediateFinal.class.getName();
  }

  public static class Initial extends AlgebraicInitial {

    /**
     * Constructor for the initial pass of an Algebraic function using default parameters.
     */
    public Initial() {}

    /**
     * Constructor for the initial pass of an Algebraic function. This will be passed the same
     * constructor arguments as the original UDF.
     * @param numValues String representation of number of values per key
     */
    public Initial(final String numValues) {}

    /**
     * Constructor for the initial pass of an Algebraic function. This will be passed the same
     * constructor arguments as the original UDF.
     * @param sketchSize String representation of sketch size
     * @param numValues String representation of number of values per key
     */
    public Initial(final String sketchSize, final String numValues) {}

  }

  public static class IntermediateFinal extends UnionArrayOfDoublesSketchAlgebraicIntermediateFinal {

    /**
     * Constructor for the intermediate and final passes of an Algebraic function.
     * Default parameters.
     */
    public IntermediateFinal() {
      super();
    }

    /**
     * Constructor for the intermediate and final passes of an Algebraic function. This will be
     * passed the same constructor arguments as the original UDF.
     * @param numValues String representation of number of values per key
     */
    public IntermediateFinal(final String numValues) {
      super(Integer.parseInt(numValues));
    }

    /**
     * Constructor for the intermediate and final passes of an Algebraic function. This will be
     * passed the same constructor arguments as the original UDF.
     * @param sketchSize String representation of sketch size
     * @param numValues String representation of number of values per key
     */
    public IntermediateFinal(final String sketchSize, final String numValues) {
      super(Integer.parseInt(sketchSize), Integer.parseInt(numValues));
    }

  }

}
