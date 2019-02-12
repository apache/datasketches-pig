/*
 * Copyright 2019, Verizon Media.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.pig.quantiles;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.quantiles.DoublesSketch;

/**
 * This UDF is to get a human-readable summary of a given sketch.
 */
public class DoublesSketchToString extends EvalFunc<String> {

  @Override
  public String exec(final Tuple input) throws IOException {
    if (input == null) {
      return null;
    }
    if (input.size() != 1) {
      throw new IllegalArgumentException("expected one input");
    }

    if (!(input.get(0) instanceof DataByteArray)) {
      throw new IllegalArgumentException("expected a DataByteArray as a sketch, got "
          + input.get(0).getClass().getSimpleName());
    }
    final DataByteArray dba = (DataByteArray) input.get(0);
    final DoublesSketch sketch = DoublesSketch.wrap(Memory.wrap(dba.get()));

    return sketch.toString();
  }

}
