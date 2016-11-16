/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.pig.quantiles;

import java.util.Comparator;

import com.yahoo.sketches.ArrayOfItemsSerDe;
import com.yahoo.sketches.ArrayOfStringsSerDe;

/**
 * Creates an ItemsSketch&lt;String&gt; from raw data.
 * It supports all three ways: exec(), Accumulator and Algebraic.
 */
public class DataToStringsSketch extends DataToItemsSketch<String> {

  private static final Comparator<String> COMPARATOR = Comparator.naturalOrder();
  private static final ArrayOfItemsSerDe<String> SER_DE = new ArrayOfStringsSerDe();

  public DataToStringsSketch() {
    super(0, COMPARATOR, SER_DE);
  }

  public DataToStringsSketch(final String kStr) {
    super(Integer.parseInt(kStr), COMPARATOR, SER_DE);
  }

  // ALGEBRAIC INTERFACE

  @Override
  public String getInitial() {
    return DataToItemsSketchInitial.class.getName();
  }

  @Override
  public String getIntermed() {
    return DataToStringsSketchIntermediateFinal.class.getName();
  }

  @Override
  public String getFinal() {
    return DataToStringsSketchIntermediateFinal.class.getName();
  }

  public static class DataToStringsSketchIntermediateFinal
      extends DataToItemsSketchIntermediateFinal<String> {

    public DataToStringsSketchIntermediateFinal() {
      super(0, COMPARATOR, SER_DE);
    }

    public DataToStringsSketchIntermediateFinal(final String kStr) {
      super(Integer.parseInt(kStr), COMPARATOR, SER_DE);
    }

  }

}
