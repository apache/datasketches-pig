/*
 * Copyright 2017, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.pig.tuple;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.quantiles.DoublesSketchBuilder;
import com.yahoo.sketches.quantiles.UpdateDoublesSketch;
import com.yahoo.sketches.tuple.ArrayOfDoublesSketch;
import com.yahoo.sketches.tuple.ArrayOfDoublesSketchIterator;
import com.yahoo.sketches.tuple.ArrayOfDoublesSketches;

/**
 * This UDF converts a given column of double values from an ArrayOfDoubles sketch
 * to a quantiles DoublesSketch to further analyze the distribution of these values.
 * The result will be a DataByteArray with serialized quantiles sketch.
 */
public class ArrayOfDoublesSketchToQuantilesSketch extends EvalFunc<DataByteArray> {

  private final int k;

  /**
   * Constructor with default parameter k for quantiles sketch
   */
  public ArrayOfDoublesSketchToQuantilesSketch() {
    k = 0;
  }

  /**
   * Constructor with a given parameter k for quantiles sketch
   * @param k parameter that determines the accuracy and size of the quantiles sketch
   */
  public ArrayOfDoublesSketchToQuantilesSketch(final int k) {
    this.k = k;
  }

  @Override
  public DataByteArray exec(final Tuple input) throws IOException {
    if ((input == null) || (input.size() == 0)) {
      return null;
    }

    final DataByteArray dba = (DataByteArray) input.get(0);
    final ArrayOfDoublesSketch sketch = ArrayOfDoublesSketches.wrapSketch(Memory.wrap(dba.get()));

    int column = 1;
    if (input.size() > 1) {
      column = (int) input.get(1);
      if (column < 1 || column > sketch.getNumValues()) {
        throw new IllegalArgumentException("Column number out of range. The given sketch has "
          + sketch.getNumValues() + " columns");
      }
    }

    final DoublesSketchBuilder builder = UpdateDoublesSketch.builder();
    if (k > 0) {
      builder.setK(k);
    }
    final UpdateDoublesSketch qs = builder.build();
    final ArrayOfDoublesSketchIterator it = sketch.iterator();
    while (it.next()) {
      qs.update(it.getValues()[column - 1]);
    }
    return new DataByteArray(qs.compact().toByteArray());
  }

}
