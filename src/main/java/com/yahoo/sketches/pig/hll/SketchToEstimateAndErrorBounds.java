/*
 * Copyright 2017, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.pig.hll;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.yahoo.sketches.hll.HllSketch;

/**
 * This is a User Defined Function (UDF) for obtaining the unique count estimate
 * along with a lower and upper bound from an HllSketch.
 *
 * <p>The result is a tuple with three double values: estimate, lower bound and upper bound.
 * The bounds are given at 95.5% confidence.
 *
 * @author Alexander Saydakov
 */
public class SketchToEstimateAndErrorBounds extends EvalFunc<Tuple> {

  @Override
  public Tuple exec(final Tuple sketchTuple) throws IOException {
    if ((sketchTuple == null) || (sketchTuple.size() == 0)) {
      return null;
    }
    final DataByteArray dba = (DataByteArray) sketchTuple.get(0);
    final HllSketch sketch = HllSketch.heapify(dba.get());
    final Tuple outputTuple = TupleFactory.getInstance().newTuple(3);
    outputTuple.set(0, Double.valueOf(sketch.getEstimate()));
    outputTuple.set(1, Double.valueOf(sketch.getLowerBound(2)));
    outputTuple.set(2, Double.valueOf(sketch.getUpperBound(2)));
    return outputTuple;
  }

  /**
   * The output is a Sketch Result Tuple Schema.
   */
  @Override
  public Schema outputSchema(final Schema input) {
    if (input == null) { return null; }
    try {
      final Schema tupleSchema = new Schema();
      tupleSchema.add(new Schema.FieldSchema("Estimate", DataType.DOUBLE));
      tupleSchema.add(new Schema.FieldSchema("LowerBound", DataType.DOUBLE));
      tupleSchema.add(new Schema.FieldSchema("UpperBound", DataType.DOUBLE));
      return new Schema(new Schema.FieldSchema(getSchemaName(this
          .getClass().getName().toLowerCase(), input), tupleSchema, DataType.TUPLE));
    } catch (final FrontendException e) {
      throw new RuntimeException(e);
    }
  }

}
