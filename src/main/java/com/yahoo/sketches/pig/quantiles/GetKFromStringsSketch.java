/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.pig.quantiles;

import java.io.IOException;
import java.util.Comparator;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

import com.yahoo.sketches.ArrayOfStringsSerDe;
import com.yahoo.sketches.memory.NativeMemory;
import com.yahoo.sketches.quantiles.ItemsSketch;

public class GetKFromStringsSketch extends EvalFunc<Integer> {

  @Override
  public Integer exec(final Tuple input) throws IOException {
    if (input.size() != 1) throw new IllegalArgumentException("expected one input only");

    if (!(input.get(0) instanceof DataByteArray)) throw new IllegalArgumentException("expected a DataByteArray as a sketch, got " + input.get(0).getClass().getSimpleName());
    final DataByteArray dba = (DataByteArray) input.get(0);
    final ItemsSketch<String> sketch = ItemsSketch.getInstance(new NativeMemory(dba.get()), Comparator.naturalOrder(), new ArrayOfStringsSerDe());

    return sketch.getK();
  }

}
