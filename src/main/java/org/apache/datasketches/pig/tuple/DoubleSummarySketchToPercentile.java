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
import org.apache.datasketches.quantiles.DoublesSketch;
import org.apache.datasketches.quantiles.UpdateDoublesSketch;
import org.apache.datasketches.tuple.Sketch;
import org.apache.datasketches.tuple.SketchIterator;
import org.apache.datasketches.tuple.Sketches;
import org.apache.datasketches.tuple.SummaryDeserializer;
import org.apache.datasketches.tuple.adouble.DoubleSummary;
import org.apache.datasketches.tuple.adouble.DoubleSummaryDeserializer;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

/**
 * This UDF is to get a percentile value from a Sketch&lt;DoubleSummary&gt;.
 * The values from DoubleSummary objects in the sketch are extracted,
 * and a single value with the given rank is returned. The rank is in
 * percent. For example, 50th percentile is the median value of the
 * distribution (the number separating the higher half of a probability
 * distribution from the lower half).
 */
public class DoubleSummarySketchToPercentile extends EvalFunc<Double> {

  private static final SummaryDeserializer<DoubleSummary> SUMMARY_DESERIALIZER =
      new DoubleSummaryDeserializer(); 
  private static final int QUANTILES_SKETCH_SIZE = 1024;

  @Override
  public Double exec(final Tuple input) throws IOException {
    if (input.size() != 2) {
      throw new IllegalArgumentException("expected two inputs: sketch and pecentile");
    }

    final DataByteArray dba = (DataByteArray) input.get(0);
    final Sketch<DoubleSummary> sketch = Sketches.heapifySketch(
        Memory.wrap(dba.get()), SUMMARY_DESERIALIZER);

    final double percentile = (double) input.get(1);
    if ((percentile < 0) || (percentile > 100)) {
      throw new IllegalArgumentException("percentile must be between 0 and 100");
    }

    final UpdateDoublesSketch qs = DoublesSketch.builder().setK(QUANTILES_SKETCH_SIZE).build();
    final SketchIterator<DoubleSummary> it = sketch.iterator();
    while (it.next()) {
      qs.update(it.getSummary().getValue());
    }
    return qs.getQuantile(percentile / 100);
  }

}
