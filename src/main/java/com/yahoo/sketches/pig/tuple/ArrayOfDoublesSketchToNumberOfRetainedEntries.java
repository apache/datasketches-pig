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
import com.yahoo.sketches.tuple.ArrayOfDoublesSketch;
import com.yahoo.sketches.tuple.ArrayOfDoublesSketches;

/**
 * This is a User Defined Function (UDF) for obtaining the number of retained entries
 * from an ArrayOfDoublesSketch.
 *
 * <p>The result is an integer value.
 *
 * @author Alexander Saydakov
 */
public class ArrayOfDoublesSketchToNumberOfRetainedEntries extends EvalFunc<Integer> {

  @Override
  public Integer exec(final Tuple input) throws IOException {
    if ((input == null) || (input.size() == 0)) {
      return null;
    }

    final DataByteArray dba = (DataByteArray) input.get(0);
    final ArrayOfDoublesSketch sketch = ArrayOfDoublesSketches.wrapSketch(Memory.wrap(dba.get()));

    return sketch.getRetainedEntries();
  }

}
