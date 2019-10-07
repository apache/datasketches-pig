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

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.commons.math3.stat.inference.TTest;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.tuple.ArrayOfDoublesSketch;
import org.apache.datasketches.tuple.ArrayOfDoublesSketches;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

/**
 * Calculate p-values given two ArrayOfDoublesSketch. Each value in the sketch
 * is treated as a separate metric measurement, and a p-value will be generated
 * for each metric.
 */
public class ArrayOfDoublesSketchesToPValueEstimates extends EvalFunc<Tuple> {

  @Override
  public Tuple exec(final Tuple input) throws IOException {
    if ((input == null) || (input.size() != 2)) {
      return null;
    }

    // Get the two sketches
    final DataByteArray dbaA = (DataByteArray) input.get(0);
    final DataByteArray dbaB = (DataByteArray) input.get(1);
    final ArrayOfDoublesSketch sketchA = ArrayOfDoublesSketches.wrapSketch(Memory.wrap(dbaA.get()));
    final ArrayOfDoublesSketch sketchB = ArrayOfDoublesSketches.wrapSketch(Memory.wrap(dbaB.get()));

    // Check that the size of the arrays in the sketches are the same
    if (sketchA.getNumValues() != sketchB.getNumValues()) {
      throw new IllegalArgumentException("Both sketches must have the same number of values");
    }

    // Store the number of metrics
    final int numMetrics = sketchA.getNumValues();

    // If the sketches contain fewer than 2 values, the p-value can't be calculated
    if (sketchA.getRetainedEntries() < 2 || sketchB.getRetainedEntries() < 2) {
      return null;
    }

    // Get the statistical summary from each sketch
    final SummaryStatistics[] summariesA = ArrayOfDoublesSketchStats.sketchToSummaryStatistics(sketchA);
    final SummaryStatistics[] summariesB = ArrayOfDoublesSketchStats.sketchToSummaryStatistics(sketchB);

    // Calculate the p-values
    final TTest tTest = new TTest();
    final Tuple pValues = TupleFactory.getInstance().newTuple(numMetrics);
    for (int i = 0; i < numMetrics; i++) {
      // Pass the sampled values for each metric
      pValues.set(i, tTest.tTest(summariesA[i], summariesB[i]));
    }

    return pValues;
  }

}
