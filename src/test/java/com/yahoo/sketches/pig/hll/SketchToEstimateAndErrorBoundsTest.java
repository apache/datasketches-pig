/*
 * Copyright 2017, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.pig.hll;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.sketches.hll.HllSketch;

public class SketchToEstimateAndErrorBoundsTest {

  private static final TupleFactory tupleFactory = TupleFactory.getInstance();

  @Test
  public void nullInputTuple() throws Exception {
    EvalFunc<Tuple> func = new SketchToEstimateAndErrorBounds();
    Tuple result = func.exec(null);
    Assert.assertNull(result);
  }

  @Test
  public void emptyInputTuple() throws Exception {
    EvalFunc<Tuple> func = new SketchToEstimateAndErrorBounds();
    Tuple result = func.exec(tupleFactory.newTuple());
    Assert.assertNull(result);
  }

  @Test
  public void normalCase() throws Exception {
    EvalFunc<Tuple> func = new SketchToEstimateAndErrorBounds();
    HllSketch sketch = new HllSketch(12);
    sketch.update(1);
    sketch.update(2);
    Tuple result = func.exec(tupleFactory.newTuple(new DataByteArray(sketch.toCompactByteArray())));
    Assert.assertNotNull(result);
    Assert.assertEquals((Double) result.get(0), 2.0, 0.01);
    Assert.assertTrue((Double) result.get(1) <= 2.0);
    Assert.assertTrue((Double) result.get(2) >= 2.0);
  }

  @Test
  public void schema() throws Exception {
    EvalFunc<Tuple> func = new SketchToEstimateAndErrorBounds();
    Schema inputSchema = new Schema(new Schema.FieldSchema("Sketch", DataType.BYTEARRAY));
    Schema outputSchema = func.outputSchema(inputSchema);
    Assert.assertNotNull(outputSchema);
    Assert.assertEquals(outputSchema.size(), 1);
    Assert.assertEquals(DataType.findTypeName(outputSchema.getField(0).type), "tuple");
    Schema innerSchema = outputSchema.getField(0).schema;
    Assert.assertEquals(innerSchema.size(), 3);
    Assert.assertEquals(DataType.findTypeName(innerSchema.getField(0).type), "double");
    Assert.assertEquals(DataType.findTypeName(innerSchema.getField(1).type), "double");
    Assert.assertEquals(DataType.findTypeName(innerSchema.getField(2).type), "double");
  }

}
