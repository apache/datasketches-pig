/*
 * Copyright 2017, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.pig.tuple;

import java.io.IOException;
import java.util.Arrays;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.tuple.ArrayOfDoublesSketch;
import com.yahoo.sketches.tuple.ArrayOfDoublesSketches;

/**
 * This is a User Defined Function (UDF) for obtaining the unique count estimate
 * along with a lower and upper bound from an ArrayOfDoublesSketch.
 *
 * <p>The result is a tuple with three double values: estimate, lower bound and upper bound.
 * The bounds are given at 95.5% confidence.
 *
 * @author Alexander Saydakov
 */
public class ArrayOfDoublesSketchToEstimateAndErrorBounds extends EvalFunc<Tuple> {

  @Override
  public Tuple exec(final Tuple input) throws IOException {
    if ((input == null) || (input.size() == 0)) {
      return null;
    }

    final DataByteArray dba = (DataByteArray) input.get(0);
    final ArrayOfDoublesSketch sketch = ArrayOfDoublesSketches.wrapSketch(Memory.wrap(dba.get()));

    return TupleFactory.getInstance().newTuple(Arrays.asList(
      sketch.getEstimate(),
      sketch.getLowerBound(2),
      sketch.getUpperBound(2)
    ));
  }

}
