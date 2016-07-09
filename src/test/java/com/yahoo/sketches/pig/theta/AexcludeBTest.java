/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.pig.theta;

import static com.yahoo.sketches.pig.PigTestingUtil.LS;
import static com.yahoo.sketches.pig.PigTestingUtil.createDbaFromQssRange;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.sketches.pig.theta.AexcludeB;
import com.yahoo.sketches.pig.theta.Estimate;

/**
 * @author Lee Rhodes
 */
public class AexcludeBTest {
  
  @SuppressWarnings("unused")
  @Test
  public void checkConstructors() {
    AexcludeB aNOTb = new AexcludeB();
    aNOTb = new AexcludeB("9001");
    aNOTb = new AexcludeB(9001);
  }
  
  @Test
  public void checkNullCombinations() throws IOException {
    EvalFunc<Tuple> aNbFunc = new AexcludeB();
    EvalFunc<Double> estFunc = new Estimate();
    
    Tuple inputTuple, resultTuple;
    Double est;
    //Two nulls
    inputTuple = TupleFactory.getInstance().newTuple(2);
    resultTuple = aNbFunc.exec(inputTuple);
    assertNotNull(resultTuple);
    assertEquals(resultTuple.size(), 1);
    est = estFunc.exec(resultTuple);
    assertEquals(est, 0.0, 0.0);
    
    //A is null
    inputTuple = TupleFactory.getInstance().newTuple(2);
    inputTuple.set(1, createDbaFromQssRange(256, 0, 128));
    resultTuple = aNbFunc.exec(inputTuple);
    assertNotNull(resultTuple);
    assertEquals(resultTuple.size(), 1);
    est = estFunc.exec(resultTuple);
    assertEquals(est, 0.0, 0.0);
    
    //A is valid, B is null
    inputTuple = TupleFactory.getInstance().newTuple(2);
    inputTuple.set(0, createDbaFromQssRange(256, 0, 256));
    resultTuple = aNbFunc.exec(inputTuple);
    assertNotNull(resultTuple);
    assertEquals(resultTuple.size(), 1);
    est = estFunc.exec(resultTuple);
    assertEquals(est, 256.0, 0.0);
    
    //Both valid
    inputTuple = TupleFactory.getInstance().newTuple(2);
    inputTuple.set(0, createDbaFromQssRange(256, 0, 256));
    inputTuple.set(1, createDbaFromQssRange(256, 0, 128));
    resultTuple = aNbFunc.exec(inputTuple);
    assertNotNull(resultTuple);
    assertEquals(resultTuple.size(), 1);
    est = estFunc.exec(resultTuple);
    assertEquals(est, 128.0, 0.0);
  }
  
  @Test
  public void outputSchemaTest() throws IOException {
    EvalFunc<Tuple> udf = new AexcludeB("512");
    
    Schema inputSchema = null;
    
    Schema nullOutputSchema = null;
    
    Schema outputSchema = null;
    Schema.FieldSchema outputOuterFs0 = null;
    
    Schema outputInnerSchema = null;
    Schema.FieldSchema outputInnerFs0 = null;
    
    inputSchema = Schema.generateNestedSchema(DataType.BAG, DataType.BYTEARRAY);
    
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
    
    expected = "bytearray";
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
    println(this.getClass().getSimpleName());
  }
  
  /**
   * @param s value to print 
   */
  static void println(String s) {
    //System.out.println(s); //disable here
  }
  
}