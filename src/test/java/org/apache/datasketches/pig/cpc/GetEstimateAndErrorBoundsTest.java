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

package org.apache.datasketches.pig.cpc;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.datasketches.cpc.CpcSketch;

@SuppressWarnings("javadoc")
public class GetEstimateAndErrorBoundsTest {

  private static final TupleFactory TUPLE_FACTORY = TupleFactory.getInstance();

  @Test
  public void nullInputTuple() throws Exception {
    final EvalFunc<Tuple> func = new GetEstimateAndErrorBounds();
    final Tuple result = func.exec(null);
    Assert.assertNull(result);
  }

  @Test
  public void emptyInputTuple() throws Exception {
    final EvalFunc<Tuple> func = new GetEstimateAndErrorBounds();
    final Tuple result = func.exec(TUPLE_FACTORY.newTuple());
    Assert.assertNull(result);
  }

  @Test
  public void normalCase() throws Exception {
    final EvalFunc<Tuple> func = new GetEstimateAndErrorBounds();
    final CpcSketch sketch = new CpcSketch();
    sketch.update(1);
    sketch.update(2);
    Tuple result = func.exec(TUPLE_FACTORY.newTuple(new DataByteArray(sketch.toByteArray())));
    Assert.assertNotNull(result);
    Assert.assertEquals((Double) result.get(0), 2.0, 0.01);
    Assert.assertTrue((Double) result.get(1) <= 2.0);
    Assert.assertTrue((Double) result.get(2) >= 2.0);
  }

  @Test
  public void normalCaseWithKappa() throws Exception {
    final EvalFunc<Tuple> func = new GetEstimateAndErrorBounds("1");
    final CpcSketch sketch = new CpcSketch();
    sketch.update(1);
    sketch.update(2);
    Tuple result = func.exec(TUPLE_FACTORY.newTuple(new DataByteArray(sketch.toByteArray())));
    Assert.assertNotNull(result);
    Assert.assertEquals((Double) result.get(0), 2.0, 0.01);
    Assert.assertTrue((Double) result.get(1) <= 2.0);
    Assert.assertTrue((Double) result.get(2) >= 2.0);
  }

  @Test
  public void normalCaseWithKappaAndSeed() throws Exception {
    final EvalFunc<Tuple> func = new GetEstimateAndErrorBounds("3", "123");
    final CpcSketch sketch = new CpcSketch(12, 123);
    sketch.update(1);
    sketch.update(2);
    Tuple result = func.exec(TUPLE_FACTORY.newTuple(new DataByteArray(sketch.toByteArray())));
    Assert.assertNotNull(result);
    Assert.assertEquals((Double) result.get(0), 2.0, 0.01);
    Assert.assertTrue((Double) result.get(1) <= 2.0);
    Assert.assertTrue((Double) result.get(2) >= 2.0);
  }

  @Test
  public void schema() throws Exception {
    EvalFunc<Tuple> func = new GetEstimateAndErrorBounds();
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
