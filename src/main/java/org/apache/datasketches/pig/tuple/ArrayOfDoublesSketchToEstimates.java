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

package org.apache.datasketches.pig.tuple;

import java.io.IOException;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.tuple.ArrayOfDoublesSketch;
import org.apache.datasketches.tuple.ArrayOfDoublesSketchIterator;
import org.apache.datasketches.tuple.ArrayOfDoublesSketches;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

/**
 * This UDF converts an ArrayOfDoubles sketch to estimates.
 * The result will be a tuple with N + 1 double values, where
 * N is the number of double values kept in the sketch per key.
 * The first estimate is the estimate of the number of unique
 * keys in the original population.
 * Next there are N estimates of the sums of the parameters
 * in the original population (sums of the values in the sketch
 * scaled to the original population).
 */
public class ArrayOfDoublesSketchToEstimates extends EvalFunc<Tuple> {

  @Override
  public Tuple exec(final Tuple input) throws IOException {
    if ((input == null) || (input.size() == 0)) {
      return null;
    }

    final DataByteArray dba = (DataByteArray) input.get(0);
    final ArrayOfDoublesSketch sketch = ArrayOfDoublesSketches.wrapSketch(Memory.wrap(dba.get()));

    final double[] estimates = new double[sketch.getNumValues() + 1];
    estimates[0] = sketch.getEstimate();
    if (sketch.getRetainedEntries() > 0) { // remove unnecessary check when version of sketches-core > 0.4.0
      final ArrayOfDoublesSketchIterator it = sketch.iterator();
      while (it.next()) {
        final double[] values = it.getValues();
        for (int i = 0; i < sketch.getNumValues(); i++) {
          estimates[i + 1] += values[i];
        }
      }
      for (int i = 0; i < sketch.getNumValues(); i++) {
        estimates[i + 1] /= sketch.getTheta();
      }
    }
    return Util.doubleArrayToTuple(estimates);
  }
}
