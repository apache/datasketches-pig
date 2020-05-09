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

package org.apache.datasketches.pig.hash;

import static org.apache.datasketches.pig.PigTestingUtil.LS;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Tests the MurmurHash3 class.
 */
@SuppressWarnings({"unchecked", "javadoc" })
public class MurmurHash3Test {
  private static final TupleFactory mTupleFactory = TupleFactory.getInstance();

  private String hashUdfName = "org.apache.datasketches.pig.hash.MurmurHash3";

  @Test
  public void checkExceptions1() throws IOException {
    EvalFunc<Tuple> hashUdf =
        (EvalFunc<Tuple>) PigContext.instantiateFuncFromSpec(new FuncSpec(hashUdfName));
    Tuple in, out;
    //Empty input tuple
    in = mTupleFactory.newTuple(0);
    out = hashUdf.exec(in);
    Assert.assertNull(out);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkExceptions2() throws IOException {
    EvalFunc<Tuple> hashUdf =
        (EvalFunc<Tuple>) PigContext.instantiateFuncFromSpec(new FuncSpec(hashUdfName));
    Tuple in, out;
    //seed must be INTEGER or LONG
    in = mTupleFactory.newTuple(2);
    in.set(0, "ABC");
    in.set(1, Double.valueOf(9001));
    out = hashUdf.exec(in);
    assertNotNull(out);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkExceptions3() throws IOException {
    EvalFunc<Tuple> hashUdf =
        (EvalFunc<Tuple>) PigContext.instantiateFuncFromSpec(new FuncSpec(hashUdfName));
    Tuple in, out;
    //improper hash object = Tuple
    in = mTupleFactory.newTuple(1);
    in.set(0, in);
    out = hashUdf.exec(in);
    assertNotNull(out);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkExceptions4() throws IOException {
    EvalFunc<Tuple> hashUdf =
        (EvalFunc<Tuple>) PigContext.instantiateFuncFromSpec(new FuncSpec(hashUdfName));
    Tuple in, out;
    //divisor must be INTEGER
    in = mTupleFactory.newTuple(3);
    in.set(0, "ABC");
    in.set(1, 0);
    in.set(2, Long.valueOf(8));
    out = hashUdf.exec(in);
    assertNotNull(out);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkExceptions5() throws IOException {
    EvalFunc<Tuple> hashUdf =
        (EvalFunc<Tuple>) PigContext.instantiateFuncFromSpec(new FuncSpec(hashUdfName));
    Tuple in, out;
    //divisor must be INTEGER > 0
    in = mTupleFactory.newTuple(3);
    in.set(0, "ABC");
    in.set(1, 0);
    in.set(2, Integer.valueOf(0));
    out = hashUdf.exec(in);
    assertNotNull(out);
  }

  @Test
  public void check1ValidArg() throws IOException {
    EvalFunc<Tuple> hashUdf =
        (EvalFunc<Tuple>) PigContext.instantiateFuncFromSpec(new FuncSpec(hashUdfName));
    Tuple in, out;

    //test Integer, Long, Float, Double, DataByteArray, String
    in = mTupleFactory.newTuple(1);

    in.set(0, null);
    out = hashUdf.exec(in);
    Assert.assertNull(out.get(0));
    Assert.assertNull(out.get(1));
    Assert.assertNull(out.get(2));

    in.set(0, Integer.valueOf(1));
    out = hashUdf.exec(in);
    checkOutput(out, false);

    in.set(0, Long.valueOf(1));
    out = hashUdf.exec(in);
    checkOutput(out, false);

    in.set(0, Float.valueOf(1.0f));
    out = hashUdf.exec(in);
    checkOutput(out, false);

    in.set(0, Double.valueOf(0.0));
    out = hashUdf.exec(in);
    checkOutput(out, false);

    in.set(0, Double.valueOf( -0.0));
    out = hashUdf.exec(in);
    checkOutput(out, false);

    in.set(0, Double.NaN);
    out = hashUdf.exec(in);
    checkOutput(out, false);

    in.set(0, "1");
    out = hashUdf.exec(in);
    checkOutput(out, false);

    in.set(0, ""); //empty
    out = hashUdf.exec(in);
    Assert.assertNull(out.get(0));
    Assert.assertNull(out.get(1));
    Assert.assertNull(out.get(2));

    byte[] bArr = { 1, 2, 3, 4 };
    DataByteArray dba = new DataByteArray(bArr);
    in.set(0, dba);
    out = hashUdf.exec(in);
    checkOutput(out, false);

    bArr = new byte[0]; //empty
    dba = new DataByteArray(bArr);
    in.set(0, dba);
    out = hashUdf.exec(in);
    Assert.assertNull(out.get(0));
    Assert.assertNull(out.get(1));
    Assert.assertNull(out.get(2));
  }

  @Test
  public void check2ValidArg() throws IOException {
    EvalFunc<Tuple> hashUdf =
        (EvalFunc<Tuple>) PigContext.instantiateFuncFromSpec(new FuncSpec(hashUdfName));
    Tuple in, out;

    //test String, seed
    in = mTupleFactory.newTuple(2);

    in.set(0, "1");
    //2nd is null
    out = hashUdf.exec(in);
    checkOutput(out, false);

    in.set(0, "1");
    in.set(1, 9001);
    out = hashUdf.exec(in);
    checkOutput(out, false);

    in.set(0, "1");
    in.set(1, 9001L);
    out = hashUdf.exec(in);
    checkOutput(out, false);
  }

  @Test
  public void check3ValidArg() throws IOException {
    EvalFunc<Tuple> hashUdf =
        (EvalFunc<Tuple>) PigContext.instantiateFuncFromSpec(new FuncSpec(hashUdfName));
    Tuple in, out;

    //test String, seed
    in = mTupleFactory.newTuple(3);

    in.set(0, "1");
    //2nd is null
    //3rd is null
    out = hashUdf.exec(in);
    checkOutput(out, false);

    in.set(0, "1");
    in.set(1, 9001);
    //3rd is null
    out = hashUdf.exec(in);
    checkOutput(out, false);

    in.set(0, "1");
    in.set(1, 9001);
    in.set(2, 7);
    out = hashUdf.exec(in);
    checkOutput(out, true);
  }

  @Test
  public void check3ValidArgs() throws IOException {
    EvalFunc<Tuple> hashUdf =
        (EvalFunc<Tuple>) PigContext.instantiateFuncFromSpec(new FuncSpec(hashUdfName));
    Tuple in, out;

    //test multiple integers, seed
    in = mTupleFactory.newTuple(3);

    for (int i = 0; i < 10; i++ ) {
      in.set(0, i);
      in.set(1, 9001);
      in.set(2, 7);
      out = hashUdf.exec(in);
      checkOutput(out, true);
    }
  }

  private static void checkOutput(Tuple out, boolean checkMod) throws IOException {
    long h0 = (Long) out.get(0);
    long h1 = (Long) out.get(1);
    Assert.assertNotEquals(h0, 0L);
    Assert.assertNotEquals(h1, 0L);
    if (checkMod) {
      int r = (Integer) out.get(2);
      Assert.assertTrue(r >= 0, "" + r);
    }
  }

  /**
   * Test the outputSchema method for MurmurHash3.
   * @throws IOException thrown by Pig
   */
  @Test
  public void outputSchemaTestMurmurHash3Udf() throws IOException {
    EvalFunc<Tuple> hashUdf =
        (EvalFunc<Tuple>) PigContext.instantiateFuncFromSpec(new FuncSpec(hashUdfName));

    Schema inputSchema = null;

    Schema nullOutputSchema = null;

    Schema outputSchema = null;
    Schema.FieldSchema outputOuterFs0 = null;

    Schema outputInnerSchema = null;
    Schema.FieldSchema outputInnerFs0 = null;
    Schema.FieldSchema outputInnerFs1 = null;
    Schema.FieldSchema outputInnerFs2 = null;

    nullOutputSchema = hashUdf.outputSchema(null);

    //CHARARRAY is one of many different input types
    inputSchema = Schema.generateNestedSchema(DataType.BAG, DataType.CHARARRAY);

    outputSchema = hashUdf.outputSchema(inputSchema);
    outputOuterFs0 = outputSchema.getField(0);

    outputInnerSchema = outputOuterFs0.schema;
    outputInnerFs0 = outputInnerSchema.getField(0);
    outputInnerFs1 = outputInnerSchema.getField(1);
    outputInnerFs2 = outputInnerSchema.getField(2);

    Assert.assertNull(nullOutputSchema, "Should be null");
    Assert.assertNotNull(outputOuterFs0, "outputSchema.getField(0) may not be null");

    String expected = "tuple";
    String result = DataType.findTypeName(outputOuterFs0.type);
    Assert.assertEquals(result, expected);

    expected = "long";
    Assert.assertNotNull(outputInnerFs0, "innerSchema.getField(0) may not be null");
    result = DataType.findTypeName(outputInnerFs0.type);
    Assert.assertEquals(result, expected);

    expected = "long";
    Assert.assertNotNull(outputInnerFs1, "innerSchema.getField(1) may not be null");
    result = DataType.findTypeName(outputInnerFs1.type);
    Assert.assertEquals(result, expected);

    expected = "int";
    Assert.assertNotNull(outputInnerFs2, "innerSchema.getField(2) may not be null");
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
