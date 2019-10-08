/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datasketches.pig.quantiles;

import java.io.IOException;
import java.util.Arrays;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.quantiles.DoublesSketch;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

/**
 * This UDF is to get a list of quantile values from an DoublesSketch given a list of
 * fractions or a number of evenly spaced intervals. The fractions represent normalized ranks and
 * must be from 0 to 1 inclusive. For example, the fraction of 0.5 corresponds to 50th percentile,
 * which is the median value of the distribution (the number separating the higher half
 * of the probability distribution from the lower half).
 */
public class GetQuantilesFromDoublesSketch extends EvalFunc<Tuple> {

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
    final DoublesSketch sketch = DoublesSketch.wrap(Memory.wrap(dba.get()));

    if (input.size() == 2) {
      final Object arg = input.get(1);
      if (arg instanceof Integer) { // number of evenly spaced intervals
        return Util.doubleArrayToTuple(sketch.getQuantiles((int) arg));
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
    return Util.doubleArrayToTuple(sketch.getQuantiles(fractions));
  }

}
