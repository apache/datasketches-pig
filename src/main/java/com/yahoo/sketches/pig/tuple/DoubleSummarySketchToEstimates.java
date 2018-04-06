/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.pig.tuple;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.tuple.DoubleSummary;
import com.yahoo.sketches.tuple.DoubleSummaryDeserializer;
import com.yahoo.sketches.tuple.Sketch;
import com.yahoo.sketches.tuple.SketchIterator;
import com.yahoo.sketches.tuple.Sketches;
import com.yahoo.sketches.tuple.SummaryDeserializer;

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
