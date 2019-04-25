/*
 * Copyright 2019, Verizon Media.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.pig.kll;

import java.io.IOException;
import java.util.Arrays;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.kll.KllFloatsSketch;

/**
 * This UDF is to get a list of quantile values from a given sketch and a list of
 * fractions or a number of evenly spaced intervals. The fractions represent normalized ranks and
 * must be from 0 to 1 inclusive. For example, the fraction of 0.5 corresponds to 50th percentile,
 * which is the median value of the distribution (the number separating the higher half
 * of the probability distribution from the lower half).
 */
public class GetQuantiles extends EvalFunc<Tuple> {

  @Override
  public Tuple exec(final Tuple input) throws IOException {
    if (input.size() < 2) {
      throw new IllegalArgumentException("expected two or more inputs: sketch and list of fractions");
    }

    if (!(input.get(0) instanceof DataByteArray)) {
      throw new IllegalArgumentException("expected a DataByteArray as a sketch, got "
          + input.get(0).getClass().getSimpleName());
    }
    final DataByteArray dba = (DataByteArray) input.get(0);
    final KllFloatsSketch sketch = KllFloatsSketch.heapify(Memory.wrap(dba.get()));

    if (input.size() == 2) {
      final Object arg = input.get(1);
      if (arg instanceof Integer) { // number of evenly spaced intervals
        return floatArrayToTuple(sketch.getQuantiles((int) arg));
      } else if (arg instanceof Double) { // just one fraction
        return TupleFactory.getInstance().newTuple(Arrays.asList(sketch.getQuantile((double) arg)));
      } else {
        throw new IllegalArgumentException("expected a double value as a fraction or an integer value"
            + " as a number of evenly spaced intervals, got " + arg.getClass().getSimpleName());
      }
    }
    // more than one number - must be double fractions
    final double[] fractions = new double[input.size() - 1];
    for (int i = 1; i < input.size(); i++) {
      if (!(input.get(i) instanceof Double)) {
        throw new IllegalArgumentException("expected a double value as a fraction, got "
            + input.get(i).getClass().getSimpleName());
      }
      fractions[i - 1] = (double) input.get(i);
    }
    return floatArrayToTuple(sketch.getQuantiles(fractions));
  }

  private static Tuple floatArrayToTuple(final float[] array) throws ExecException {
    final Tuple tuple = TupleFactory.getInstance().newTuple(array.length);
    for (int i = 0; i < array.length; i++) {
      tuple.set(i, array[i]);
    }
    return tuple;
  }

}
