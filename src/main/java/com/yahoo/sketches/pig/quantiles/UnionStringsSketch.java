/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.pig.quantiles;

import java.util.Comparator;

import com.yahoo.sketches.ArrayOfItemsSerDe;
import com.yahoo.sketches.ArrayOfStringsSerDe;

/**
 * Computes union of ItemsSketch&lt;String&gt;.
 * It supports all three ways: exec(), Accumulator and Algebraic
 */
public class UnionStringsSketch extends UnionItemsSketch<String> {

  private static final Comparator<String> COMPARATOR = Comparator.naturalOrder();
  private static final ArrayOfItemsSerDe<String> SER_DE = new ArrayOfStringsSerDe();

  public UnionStringsSketch() {
    super(0, COMPARATOR, SER_DE);
  }

  public UnionStringsSketch(final String kStr) {
    super(Integer.parseInt(kStr), COMPARATOR, SER_DE);
  }

  //ALGEBRAIC INTERFACE

  @Override
  public String getInitial() {
    return UnionItemsSketchInitial.class.getName();
  }

  @Override
  public String getIntermed() {
    return UnionStringsSketchIntermediateFinal.class.getName();
  }

  @Override
  public String getFinal() {
    return UnionStringsSketchIntermediateFinal.class.getName();
  }

  public static class UnionStringsSketchIntermediateFinal extends UnionItemsSketchIntermediateFinal<String> {

    public UnionStringsSketchIntermediateFinal() {
      super(0, COMPARATOR, SER_DE);
    }

    public UnionStringsSketchIntermediateFinal(final String kStr) {
      super(Integer.parseInt(kStr), COMPARATOR, SER_DE);
    }

  }

}
