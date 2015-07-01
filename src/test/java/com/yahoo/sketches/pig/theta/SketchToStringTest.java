/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.pig.theta;

import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;
import static com.yahoo.sketches.pig.PigTestingUtil.LS;
import static com.yahoo.sketches.pig.PigTestingUtil.createDbaFromQssRange;
import static org.testng.Assert.assertFalse;
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

import com.yahoo.sketches.pig.theta.SketchToString;

/**
 * @author Lee Rhodes
 */
public class SketchToStringTest {
  
  @Test
  public void testNullEmpty() throws IOException {
    EvalFunc<String> func = new SketchToString("false");
    Tuple dataTuple = null;
    String result = func.exec(dataTuple);
    assertNull(result);
    
    dataTuple = TupleFactory.getInstance().newTuple(0);
    result = func.exec(dataTuple);
    assertNull(result);
  }
  
  @Test
  public void testExactNoDetail() throws IOException {
    EvalFunc<String> func = new SketchToString("false");
    
    Tuple dataTuple = TupleFactory.getInstance().newTuple(1);
    dataTuple.set(0, createDbaFromQssRange(64, 0, 64));
    
    String result = func.exec(dataTuple);
    assertNotNull(result);
    assertTrue(result.contains("SUMMARY"));
    assertFalse(result.contains("SKETCH DATA DETAIL"));
  }
  
  @Test
  public void testExactNoDetailWithSeed() throws IOException {
    EvalFunc<String> func = new SketchToString("false", Long.toString(DEFAULT_UPDATE_SEED));
    
    Tuple dataTuple = TupleFactory.getInstance().newTuple(1);
    dataTuple.set(0, createDbaFromQssRange(64, 0, 64));
    
    String result = func.exec(dataTuple);
    assertNotNull(result);
    assertTrue(result.contains("SUMMARY"));
    assertFalse(result.contains("SKETCH DATA DETAIL"));
  }
  
  @Test
  public void testExactWithDetail() throws IOException {
    EvalFunc<String> func = new SketchToString("true");
    
    Tuple dataTuple = TupleFactory.getInstance().newTuple(1);
    dataTuple.set(0, createDbaFromQssRange(64, 0, 64));
    
    String result = func.exec(dataTuple);
    assertNotNull(result);
    assertTrue(result.contains("SUMMARY"));
    assertTrue(result.contains("SKETCH DATA DETAIL"));
  }
  
  @SuppressWarnings("null")
  @Test
  public void outputSchemaTest() throws IOException {
    EvalFunc<String> udf = new SketchToString();
    
    Schema inputSchema = null;
    Schema.FieldSchema inputFieldSchema = new Schema.FieldSchema("Sketch", DataType.BYTEARRAY);
    
    Schema nullOutputSchema = null;
    
    Schema outputSchema = null;
    Schema.FieldSchema outputOuterFs0 = null;
    
    Schema outputInnerSchema = null;
    Schema.FieldSchema outputInnerFs0 = null;
    
    inputSchema = new Schema(inputFieldSchema);
    
    nullOutputSchema = udf.outputSchema(null);
    
    outputSchema = udf.outputSchema(inputSchema);
    outputOuterFs0 = outputSchema.getField(0);
    
    outputInnerSchema = outputOuterFs0.schema;
    outputInnerFs0 = outputInnerSchema.getField(0);
    
    Assert.assertNull(nullOutputSchema, "Should be null");
    Assert.assertNotNull(outputOuterFs0, "outputSchema.getField(0) schema may not be null");
    
    String expected = "tuple";
    String result = DataType.findTypeName(outputOuterFs0.type);
    Assert.assertEquals(result, expected);
    
    expected = "chararray";
    Assert.assertNotNull(outputInnerFs0, "innerSchema.getField(0) schema may not be null");
    result = DataType.findTypeName(outputInnerFs0.type);
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
        .append(", type: ").append(DataType.findTypeName(outputInnerFs0.type)).append(LS);
    println(sb.toString());
    //@formatter:on
    //end print schemas
  }
  
  @Test
  public void printlnTest() {
    println("Test");
  }
  
  /**
   * @param s value to print 
   */
  static void println(String s) {
    //System.out.println(s); //disable here
  }
  
}