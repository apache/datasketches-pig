/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.pig.theta;

import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;
import static com.yahoo.sketches.pig.PigTestingUtil.createDbaFromQssRange;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
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
