/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.pig.theta;

import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;
import static com.yahoo.sketches.pig.PigTestingUtil.createDbaFromQssRange;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.testng.annotations.Test;

import com.yahoo.sketches.pig.theta.Estimate;

/**
 * @author Lee Rhodes
 */
public class EstimateTest {

  @Test
  public void testNullEmpty() throws IOException {
    EvalFunc<Double> func = new Estimate();
    Tuple inputTuple = null;
    Double returnValue = func.exec(inputTuple);
    assertNull(returnValue);
    
    inputTuple = TupleFactory.getInstance().newTuple(0);
    returnValue = func.exec(inputTuple);
    assertNull(returnValue);
  }
  
  @Test
  public void testExact() throws IOException {
    EvalFunc<Double> func = new Estimate();

    Tuple dataTuple = TupleFactory.getInstance().newTuple(1);
    dataTuple.set(0, createDbaFromQssRange(64, 0, 64));

    Double result = func.exec(dataTuple);
    assertNotNull(result);
    assertEquals(result, 64.0, 0.0);
  }
  
  @Test
  public void testExactWithSeed() throws IOException {
    EvalFunc<Double> func = new Estimate(Long.toString(DEFAULT_UPDATE_SEED));

    Tuple dataTuple = TupleFactory.getInstance().newTuple(1);
    dataTuple.set(0, createDbaFromQssRange(64, 0, 64));

    Double result = func.exec(dataTuple);
    assertNotNull(result);
    assertEquals(result, 64.0, 0.0);
  }
  
}