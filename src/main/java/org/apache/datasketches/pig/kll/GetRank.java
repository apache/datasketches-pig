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
 * This UDF is to get a normalized rank for a given value from a given sketch. A single
 * rank for a given value is returned. The normalized rank is a double value
 * from 0 to 1 inclusive. For example, the rank of 0.5 corresponds to 50th percentile,
 * which is the median value of the distribution (the number separating the higher half
 * of the probability distribution from the lower half).
 */
public class GetRank extends EvalFunc<Double> {

  @Override
  public Double exec(final Tuple input) throws IOException {
    if (input.size() != 2) {
      throw new IllegalArgumentException("expected two inputs: sketch and value");
    }

    if (!(input.get(0) instanceof DataByteArray)) {
      throw new IllegalArgumentException("expected a DataByteArray as a sketch, got "
          + input.get(0).getClass().getSimpleName());
    }
    final DataByteArray dba = (DataByteArray) input.get(0);
    final KllFloatsSketch sketch = KllFloatsSketch.heapify(Memory.wrap(dba.get()));

    if (!(input.get(1) instanceof Float)) {
      throw new IllegalArgumentException("expected a float value, got "
          + input.get(1).getClass().getSimpleName());
    }
    final float value = (float) input.get(1);
    return sketch.getRank(value);
  }

}
