/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.pig.tuple;

import org.apache.pig.Algebraic;

import com.yahoo.sketches.tuple.adouble.DoubleSummary;
import com.yahoo.sketches.tuple.adouble.DoubleSummaryDeserializer;
import com.yahoo.sketches.tuple.adouble.DoubleSummaryFactory;
import com.yahoo.sketches.tuple.adouble.DoubleSummarySetOperations;

/**
 * This UDF creates a Sketch&lt;DoubleSummary&gt; from raw data.
 * It supports all three ways: exec(), Accumulator and Algebraic.
 */
public class DataToDoubleSummarySketch extends DataToSketch<Double, DoubleSummary> implements Algebraic {
  /**
   * Constructor with default sketch size and default mode (sum)
   */
  public DataToDoubleSummarySketch() {
    super(new DoubleSummaryFactory());
  }

  /**
   * Constructor with default mode (sum)
   * @param sketchSize String representation of sketch size
   */
  public DataToDoubleSummarySketch(final String sketchSize) {
    super(Integer.parseInt(sketchSize), new DoubleSummaryFactory());
  }

  /**
   * Constructor
   * @param sketchSize String representation of sketch size
   * @param summaryMode String representation of mode (sum, min or max)
   */
  public DataToDoubleSummarySketch(final String sketchSize, final String summaryMode) {
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
     * Constructor wit default sketch size and default mode
     */
    public Initial() {}

    /**
     * Constructor for the initial pass of an Algebraic function. This will be passed the same
     * constructor arguments as the original UDF.
     * @param sketchSize String representation of sketch size
     */
    public Initial(final String sketchSize) {}

    /**
     * Constructor for the initial pass of an Algebraic function. This will be passed the same
     * constructor arguments as the original UDF.
     * @param sketchSize String representation of sketch size
     * @param summaryMode String representation of mode (sum, min or max)
     */
    public Initial(final String sketchSize, final String summaryMode) {}

  }

  public static class IntermediateFinal
      extends DataToSketchAlgebraicIntermediateFinal<Double, DoubleSummary> {

    /**
     * Constructor for the intermediate and final passes of an Algebraic function.
     * Default sketch size and default mode
     */
    public IntermediateFinal() {
      super(new DoubleSummaryFactory(), new DoubleSummarySetOperations(), new DoubleSummaryDeserializer());
    }

    /**
     * Constructor for the intermediate and final passes of an Algebraic function. This will be
     * passed the same constructor arguments as the original UDF.
     * @param sketchSize String representation of sketch size
     */
    public IntermediateFinal(final String sketchSize) {
      super(Integer.parseInt(sketchSize), new DoubleSummaryFactory(), new DoubleSummarySetOperations(),
          new DoubleSummaryDeserializer());
    }

    /**
     * Constructor for the intermediate and final passes of an Algebraic function. This will be
     * passed the same constructor arguments as the original UDF.
     * @param sketchSize String representation of sketch size
     * @param summaryMode String representation of mode (sum, min or max)
     */
    public IntermediateFinal(final String sketchSize, final String summaryMode) {
      super(Integer.parseInt(sketchSize), new DoubleSummaryFactory(DoubleSummary.Mode.valueOf(summaryMode)),
          new DoubleSummarySetOperations(DoubleSummary.Mode.valueOf(summaryMode)),
          new DoubleSummaryDeserializer());
    }

  }

}
