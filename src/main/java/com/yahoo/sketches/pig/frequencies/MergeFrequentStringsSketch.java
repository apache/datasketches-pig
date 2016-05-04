/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.pig.frequencies;

import org.apache.pig.Algebraic;

import com.yahoo.sketches.frequencies.ArrayOfStringsSerDe;

/**
 * This is to merge Sketch&lt;DoubleSummary&gt;.
 * It supports all three ways: exec(), Accumulator and Algebraic
 */
public class MergeFrequentStringsSketch extends MergeFrequentItemsSketch<String> implements Algebraic {

  /**
   * Constructor with default mode (sum)
   * @param sketchSize String representation of sketch size
   */
  public MergeFrequentStringsSketch(final String sketchSize) {
    super(Integer.parseInt(sketchSize), new ArrayOfStringsSerDe());
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
     * Default constructor to make pig validation happy.
     */
    public Initial() {}

    /**
     * Constructor for the initial pass of an Algebraic function. This will be passed the same
     * constructor arguments as the original UDF.
     * @param sketchSize String representation of sketch size
     */
    public Initial(final String sketchSize) {}
  }

  public static class IntermediateFinal extends MergeFrequentItemsSketchAlgebraicIntermediateFinal<String> {
    /**
     * Default constructor to make pig validation happy.
     */
    public IntermediateFinal() {}

    /**
     * Constructor for the intermediate and final passes of an Algebraic function. This will be
     * passed the same constructor arguments as the original UDF.
     * @param sketchSize String representation of sketch size
     */
    public IntermediateFinal(final String sketchSize) {
      super(Integer.parseInt(sketchSize), new ArrayOfStringsSerDe());
    }
  }
}
