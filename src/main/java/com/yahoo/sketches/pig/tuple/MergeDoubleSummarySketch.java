/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.pig.tuple;

import org.apache.pig.Algebraic;

import com.yahoo.sketches.tuple.DoubleSummary;
import com.yahoo.sketches.tuple.DoubleSummaryFactory;

/**
 * This is to merge Sketch&lt;DoubleSummary&gt;.
 * It supports all three ways: exec(), Accumulator and Algebraic
 */
public class MergeDoubleSummarySketch extends MergeSketch<DoubleSummary> implements Algebraic {

  /**
   * Constructor with default mode (sum)
   * @param sketchSize String representation of sketch size
   */
  public MergeDoubleSummarySketch(String sketchSize) {
    super(Integer.parseInt(sketchSize), new DoubleSummaryFactory());
  }

  /**
   * Constructor
   * @param sketchSize String representation of sketch size
   * @param summaryMode String representation of mode (sum, min or max)
   */
  public MergeDoubleSummarySketch(String sketchSize, String summaryMode) {
    super(Integer.parseInt(sketchSize), new DoubleSummaryFactory(DoubleSummary.Mode.valueOf(summaryMode)));
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
    public Initial(String sketchSize) {}

    /**
     * Constructor for the initial pass of an Algebraic function. This will be passed the same
     * constructor arguments as the original UDF.
     * @param sketchSize String representation of sketch size
     * @param summaryMode String representation of mode (sum, min or max)
     */
    public Initial(String sketchSize, String summaryMode) {}
  }

  public static class IntermediateFinal extends MergeSketchAlgebraicIntermediateFinal<DoubleSummary> {
    /**
     * Default constructor to make pig validation happy.
     */
    public IntermediateFinal() {}

    /**
     * Constructor for the intermediate and final passes of an Algebraic function. This will be
     * passed the same constructor arguments as the original UDF.
     * @param sketchSize String representation of sketch size
     */
    public IntermediateFinal(String sketchSize) {
      super(Integer.parseInt(sketchSize), new DoubleSummaryFactory());
    }

    /**
     * Constructor for the intermediate and final passes of an Algebraic function. This will be
     * passed the same constructor arguments as the original UDF.
     * @param sketchSize String representation of sketch size
     * @param summaryMode String representation of mode (sum, min or max)
     */
    public IntermediateFinal(String sketchSize, String summaryMode) {
      super(Integer.parseInt(sketchSize), new DoubleSummaryFactory(DoubleSummary.Mode.valueOf(summaryMode)));
    }
  }
}
