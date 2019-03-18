/*
 * Copyright 2019, Verizon Media.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.pig.kll;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.kll.KllFloatsSketch;

/**
 * This UDF is to get an approximation to the Probability Mass Function (PMF) of the input stream
 * given a sketch and a set of split points - an array of <i>m</i> unique, monotonically increasing
 * float values that divide the real number line into <i>m+1</i> consecutive disjoint intervals.
 * The function returns an array of m+1 doubles each of which is an approximation to the fraction
 * of the input stream values that fell into one of those intervals. Intervals are inclusive of
 * the left split point and exclusive of the right split point.
 */
public class GetPmf extends EvalFunc<Tuple> {

  @Override
  public Tuple exec(final Tuple input) throws IOException {
    if (input.size() < 2) {
      throw new IllegalArgumentException(
          "expected two or more inputs: sketch and list of split points");
    }

    if (!(input.get(0) instanceof DataByteArray)) {
      throw new IllegalArgumentException("expected a DataByteArray as a sketch, got "
          + input.get(0).getClass().getSimpleName());
    }
    final DataByteArray dba = (DataByteArray) input.get(0);
    final KllFloatsSketch sketch = KllFloatsSketch.heapify(Memory.wrap(dba.get()));

    final float[] splitPoints = new float[input.size() - 1];
    for (int i = 1; i < input.size(); i++) {
      if (!(input.get(i) instanceof Float)) {
        throw new IllegalArgumentException("expected a float value as a split point, got "
            + input.get(i).getClass().getSimpleName());
      }
      splitPoints[i - 1] = (float) input.get(i);
    }
    final double[] pmf = sketch.getPMF(splitPoints);
    if (pmf == null) { return null; }
    return doubleArrayToTuple(pmf);
  }

  static Tuple doubleArrayToTuple(final double[] array) throws ExecException {
    final Tuple tuple = TupleFactory.getInstance().newTuple(array.length);
    for (int i = 0; i < array.length; i++) {
      tuple.set(i, array[i]);
    }
    return tuple;
  }

}
