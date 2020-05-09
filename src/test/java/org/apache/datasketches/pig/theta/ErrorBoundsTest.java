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

package org.apache.datasketches.pig.theta;

import static org.apache.datasketches.Util.DEFAULT_UPDATE_SEED;
import static org.apache.datasketches.pig.PigTestingUtil.LS;
import static org.apache.datasketches.pig.PigTestingUtil.createDbaFromAlphaRange;
import static org.apache.datasketches.pig.PigTestingUtil.createDbaFromQssRange;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;

@SuppressWarnings("javadoc")
public class ErrorBoundsTest {

  @Test
  public void testNullEmpty() throws IOException {
    EvalFunc<Tuple> func = new ErrorBounds();
    Tuple dataTuple = null;
    Tuple result = func.exec(dataTuple);
    assertNull(result);

    dataTuple = TupleFactory.getInstance().newTuple(0);
    result = func.exec(dataTuple);
    assertNull(result);
  }

  @Test
  public void testExactModeBounds() throws IOException {
    EvalFunc<Tuple> func = new ErrorBounds();
    int nomEntries = 128;

    Tuple dataTuple = TupleFactory.getInstance().newTuple(1);
    dataTuple.set(0, createDbaFromQssRange(nomEntries, 0, 64));

    Tuple result = func.exec(dataTuple);
    assertNotNull(result);
    assertEquals(result.size(), 3);

    assertEquals(((Double) result.get(0)).doubleValue(), 64.0, 0.0);
    assertEquals(((Double) result.get(1)).doubleValue(), 64.0, 0.0);
    assertEquals(((Double) result.get(2)).doubleValue(), 64.0, 0.0);
  }

  @Test
  public void testEstModeBounds() throws IOException {
    EvalFunc<Tuple> func = new ErrorBounds();
    int nomEntries = 4096;

    Tuple dataTuple = TupleFactory.getInstance().newTuple(1);
    dataTuple.set(0, createDbaFromAlphaRange(nomEntries, 0, 2*nomEntries));

    Tuple result = func.exec(dataTuple);
    assertNotNull(result);
    assertEquals(result.size(), 3);
    double est = ((Double) result.get(0)).doubleValue();
    double ub = ((Double) result.get(1)).doubleValue();
    double lb = ((Double) result.get(2)).doubleValue();

    //The error of the QS sketch for this size:
    double epsilon2SD = 2.0 / Math.sqrt(2 * nomEntries);

    //System.out.println(lb+"\t"+est+"\t"+ub);
    assertTrue(2 * nomEntries < ub && 2 * nomEntries > lb);
    assertTrue(Math.abs(ub / (est + epsilon2SD * est) - 1) < .01);
    assertTrue(Math.abs(lb / (est - epsilon2SD * est) - 1) < .01);
  }

  @Test
  public void testEstModeBoundsWithSeed() throws IOException {
    EvalFunc<Tuple> func = new ErrorBounds(Long.toString(DEFAULT_UPDATE_SEED));
    int nomEntries = 4096;

    Tuple dataTuple = TupleFactory.getInstance().newTuple(1);
    dataTuple.set(0, createDbaFromAlphaRange(nomEntries, 0, 2*nomEntries));

    Tuple result = func.exec(dataTuple);
    assertNotNull(result);
    assertEquals(result.size(), 3);
    double est = ((Double) result.get(0)).doubleValue();
    double ub = ((Double) result.get(1)).doubleValue();
    double lb = ((Double) result.get(2)).doubleValue();

    //The error of the QS sketch for this size:
    double epsilon2SD = 2.0 / Math.sqrt(2 * nomEntries);

    //System.out.println(lb+"\t"+est+"\t"+ub);
    assertTrue(2 * nomEntries < ub && 2 * nomEntries > lb);
    assertTrue(Math.abs(ub / (est + epsilon2SD * est) - 1) < .01);
    assertTrue(Math.abs(lb / (est - epsilon2SD * est) - 1) < .01);
  }

  @Test
  public void outputSchemaTest() throws IOException {
    EvalFunc<Tuple> udf = new ErrorBounds();

    Schema inputSchema = null;
    Schema.FieldSchema inputFieldSchema = new Schema.FieldSchema("Sketch", DataType.BYTEARRAY);

    Schema nullOutputSchema = null;

    Schema outputSchema = null;
    Schema.FieldSchema outputOuterFs0 = null;

    Schema outputInnerSchema = null;
    Schema.FieldSchema outputInnerFs0 = null;
    Schema.FieldSchema outputInnerFs1 = null;
    Schema.FieldSchema outputInnerFs2 = null;

    inputSchema = new Schema(inputFieldSchema);

    nullOutputSchema = udf.outputSchema(null);

    outputSchema = udf.outputSchema(inputSchema);
    outputOuterFs0 = outputSchema.getField(0);
    outputInnerSchema = outputOuterFs0.schema;
    outputInnerFs0 = outputInnerSchema.getField(0);
    outputInnerFs1 = outputInnerSchema.getField(1);
    outputInnerFs2 = outputInnerSchema.getField(2);

    Assert.assertNull(nullOutputSchema, "Should be null");

    Assert.assertNotNull(outputOuterFs0, "outputSchema.getField(0) schema may not be null");

    String expected = "tuple";
    String result = DataType.findTypeName(outputOuterFs0.type);
    Assert.assertEquals(result, expected);

    expected = "double";

    Assert.assertNotNull(outputInnerFs0, "innerSchema.getField(0) schema may not be null");
    result = DataType.findTypeName(outputInnerFs0.type);
    Assert.assertEquals(result, expected);

    Assert.assertNotNull(outputInnerFs1, "innerSchema.getField(1) schema may not be null");
    result = DataType.findTypeName(outputInnerFs1.type);
    Assert.assertEquals(result, expected);

    Assert.assertNotNull(outputInnerFs2, "innerSchema.getField(2) schema may not be null");
    result = DataType.findTypeName(outputInnerFs2.type);
    Assert.assertEquals(result, expected);

    //print schemas
    //@formatter:off
    StringBuilder sb = new StringBuilder();
    sb.append("input schema: ").append(inputSchema).append(LS)
      .append("output schema: ").append(outputSchema).append(LS)
      .append("outputOuterFs: ").append(outputOuterFs0)
        .append(", type: ").append(DataType.findTypeName(outputOuterFs0.type)).append(LS)
      .append("outputInnerSchema: ").append(outputInnerSchema).append(LS)
      .append("outputInnerFs0: ").append(outputInnerFs0)
        .append(", type: ").append(DataType.findTypeName(outputInnerFs0.type)).append(LS)
      .append("outputInnerFs1: ").append(outputInnerFs1)
        .append(", type: ").append(DataType.findTypeName(outputInnerFs1.type)).append(LS)
      .append("outputInnerFs2: ").append(outputInnerFs2)
        .append(", type: ").append(DataType.findTypeName(outputInnerFs2.type)).append(LS);
    println(sb.toString());
    //@formatter:on
    //end print schemas
  }

  @Test
  public void printlnTest() {
    println(this.getClass().getSimpleName());
  }

  /**
   * @param s value to print
   */
  static void println(String s) {
    //System.out.println(s); //disable here
  }

}
