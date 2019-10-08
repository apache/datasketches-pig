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

package org.apache.datasketches.pig.kll;

import java.io.IOException;

import org.apache.datasketches.kll.KllFloatsSketch;
import org.apache.datasketches.memory.Memory;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

/**
 * This UDF is to get an approximation to the Cumulative Distribution Function (CDF) of the input stream
 * given a sketch and a set of split points - an array of <i>m</i> unique, monotonically increasing
 * float values that divide the real number line into <i>m+1</i> consecutive disjoint intervals.
 * The function returns an array of <i>m+1</i> double values, the first <i>m</i> of which are approximations
 * to the ranks of the corresponding split points (fraction of input stream values that are less than
 * a split point). The last value is always 1. CDF can also be viewed as a cumulative version of PMF.
 */
public class GetCdf extends EvalFunc<Tuple> {

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
    final double[] cdf = sketch.getCDF(splitPoints);
    if (cdf == null) { return null; }
    return GetPmf.doubleArrayToTuple(cdf);
  }

}
