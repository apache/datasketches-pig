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
import org.apache.datasketches.tuple.Sketch;
import org.apache.datasketches.tuple.SketchIterator;
import org.apache.datasketches.tuple.Sketches;
import org.apache.datasketches.tuple.SummaryDeserializer;
import org.apache.datasketches.tuple.adouble.DoubleSummary;
import org.apache.datasketches.tuple.adouble.DoubleSummaryDeserializer;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

/**
 * This UDF converts a Sketch&lt;DoubleSummary&gt; to estimates.
 * The first estimate is the estimate of the number of unique
 * keys in the original population.
 * The second is the estimate of the sum of the parameter
 * in the original population (sums of the values in the sketch
 * scaled to the original population). This estimate assumes
 * that the DoubleSummary was used in the Sum mode.
 */
public class DoubleSummarySketchToEstimates extends EvalFunc<Tuple> {

  private static final SummaryDeserializer<DoubleSummary> SUMMARY_DESERIALIZER =
      new DoubleSummaryDeserializer(); 
  
  @Override
  public Tuple exec(final Tuple input) throws IOException {
    if ((input == null) || (input.size() == 0)) {
      return null;
    }

    final DataByteArray dba = (DataByteArray) input.get(0);
    final Sketch<DoubleSummary> sketch = Sketches.heapifySketch(
        Memory.wrap(dba.get()), SUMMARY_DESERIALIZER);

    final Tuple output = TupleFactory.getInstance().newTuple(2);
    output.set(0, sketch.getEstimate());
    double sum = 0;
    final SketchIterator<DoubleSummary> it = sketch.iterator();
    while (it.next()) {
      sum += it.getSummary().getValue();
    }
    output.set(1, sum / sketch.getTheta());

    return output;
  }
}
