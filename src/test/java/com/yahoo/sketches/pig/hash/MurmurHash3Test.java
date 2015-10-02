/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.pig.hash;

import static com.yahoo.sketches.pig.PigTestingUtil.LS;

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
 * 
 * @author Lee Rhodes
 */
@SuppressWarnings({ "unused", "unchecked" })
public class MurmurHash3Test {
  private static final TupleFactory mTupleFactory = TupleFactory.getInstance();
  
  private String hashUdfName = "com.yahoo.sketches.pig.hash.MurmurHash3";
  
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
    in.set(0, new String("ABC"));
    in.set(1, new Double(9001));
    out = hashUdf.exec(in);
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
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkExceptions4() throws IOException {
    EvalFunc<Tuple> hashUdf =
        (EvalFunc<Tuple>) PigContext.instantiateFuncFromSpec(new FuncSpec(hashUdfName));
    Tuple in, out;
    //divisor must be INTEGER
    in = mTupleFactory.newTuple(3);
    in.set(0, new String("ABC"));
    in.set(1, 0);
    in.set(2, new Long(8));
    out = hashUdf.exec(in);
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkExceptions5() throws IOException {
    EvalFunc<Tuple> hashUdf =
        (EvalFunc<Tuple>) PigContext.instantiateFuncFromSpec(new FuncSpec(hashUdfName));
    Tuple in, out;
    //divisor must be INTEGER > 0
    in = mTupleFactory.newTuple(3);
    in.set(0, new String("ABC"));
    in.set(1, 0);
    in.set(2, new Integer(0));
    out = hashUdf.exec(in);
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
    
    in.set(0, new Integer(1));
    out = hashUdf.exec(in);
    checkOutput(out, false);
    
    in.set(0, new Long(1));
    out = hashUdf.exec(in);
    checkOutput(out, false);
    
    in.set(0, new Float(1));
    out = hashUdf.exec(in);
    checkOutput(out, false);
    
    in.set(0, new Double(0.0));
    out = hashUdf.exec(in);
    checkOutput(out, false);
    
    in.set(0, new Double( -0.0));
    out = hashUdf.exec(in);
    checkOutput(out, false);
    
    in.set(0, Double.NaN);
    out = hashUdf.exec(in);
    checkOutput(out, false);
    
    in.set(0, new String("1"));
    out = hashUdf.exec(in);
    checkOutput(out, false);
    
    in.set(0, new String("")); //empty
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
    
    in.set(0, new String("1"));
    //2nd is null
    out = hashUdf.exec(in);
    checkOutput(out, false);
    
    in.set(0, new String("1"));
    in.set(1, 9001);
    out = hashUdf.exec(in);
    checkOutput(out, false);
    
    in.set(0, new String("1"));
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
    
    in.set(0, new String("1"));
    //2nd is null
    //3rd is null
    out = hashUdf.exec(in);
    checkOutput(out, false);
    
    in.set(0, new String("1"));
    in.set(1, 9001);
    //3rd is null
    out = hashUdf.exec(in);
    checkOutput(out, false);
    
    in.set(0, new String("1"));
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
   * @throws IOException 
   */
  @SuppressWarnings("null")
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