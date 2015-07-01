/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.pig.theta;

import static com.yahoo.sketches.pig.PigTestingUtil.LS;
import static com.yahoo.sketches.pig.PigTestingUtil.createDbaFromQssRange;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.IOException;

import org.apache.pig.Accumulator;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.sketches.pig.theta.Estimate;
import com.yahoo.sketches.pig.theta.Merge;

/**
 * @author Lee Rhodes
 */
public class MergeTest {
//  private String udfName = "com.yahoo.sketches.pig.theta.SketchUnions";
//  private long seed_ = Util.DEFAULT_UPDATE_SEED;
  
  @SuppressWarnings("unused")
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkConstructorExceptions1() {
    Merge test = new Merge("1023");
  }
  
  @SuppressWarnings("unused")
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkConstructorExceptions3() {
    Merge test = new Merge("8");
  }
  
  @SuppressWarnings("unused")
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testConstructorExceptions4() {
    Merge test = new Merge("1024", "2.0");
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkNotDBAExcep() throws IOException {
    Merge inter = new Merge();
    //create inputTuple and a bag, add bag to inputTuple
    Tuple inputTuple = TupleFactory.getInstance().newTuple(1);
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    inputTuple.set(0, bag);
    
    Tuple innerTuple = TupleFactory.getInstance().newTuple(1);
    bag.add(innerTuple);
    inter.accumulate(inputTuple); //add empty tuple
    
    innerTuple.set(0, new Double(1.0));  //not a DBA
    inter = new Merge();
    inter.accumulate(inputTuple); //add wrong type
  }
  
  @SuppressWarnings("unused")
  @Test
  public void checkConstructors() {
    
    Merge inter = new Merge();
    inter = new Merge("1024");
    inter = new Merge("1024", "1.0");
    inter = new Merge("1024", "1.0", "9001");
    inter = new Merge(1024, (float) 1.0, 9001);
    
    Merge.Initial initial = new Merge.Initial();
    initial = new Merge.Initial("1024");
    initial = new Merge.Initial("1024", "1.0");
    initial = new Merge.Initial("1024", "1.0", "9001");
    
    Merge.IntermediateFinal interFin = new Merge.IntermediateFinal();
    interFin = new Merge.IntermediateFinal("1024");
    interFin = new Merge.IntermediateFinal("1024", "1.0");
    interFin = new Merge.IntermediateFinal("1024", "1.0", "9001");
    interFin = new Merge.IntermediateFinal(1024, (float) 1.0, 9001);
  }
  
  @Test
  public void checkNullInput() throws IOException {
    EvalFunc<Tuple> unionFunc = new Merge(); //default 4096
    EvalFunc<Double> estFunc = new Estimate();
    Tuple inputTuple = TupleFactory.getInstance().newTuple(1);
    //null bag
    
    Tuple resultTuple = unionFunc.exec(inputTuple);
    assertNotNull(resultTuple);
    assertEquals(resultTuple.size(), 1);
    
    Double est = estFunc.exec(resultTuple);
    assertEquals(est, 0.0, 0.0);
  }
  
  @Test
  public void checkExactTopExec() throws IOException {
    EvalFunc<Tuple> unionFunc = new Merge(); //default 4096
    EvalFunc<Double> estFunc = new Estimate();
    
    //create inputTuple and a bag, add bag to inputTuple
    Tuple inputTuple = TupleFactory.getInstance().newTuple(1);
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    inputTuple.set(0, bag);

    //create 4 distinct sketches of 64 in a bag
    for (int i = 0; i < 4; i++ ) {
      Tuple dataTuple = TupleFactory.getInstance().newTuple(1);
      dataTuple.set(0, createDbaFromQssRange(64, i*64, 64));

      bag.add(dataTuple);
    }

    Tuple resultTuple = unionFunc.exec(inputTuple);
    assertNotNull(resultTuple);
    assertEquals(resultTuple.size(), 1);
    
    Double est = estFunc.exec(resultTuple);
    assertEquals(est, 256.0, 0.0);
  }
  
  @Test(expectedExceptions = ClassCastException.class)
  public void checkBadClassCast() throws IOException {
    Accumulator<Tuple> unionFunc = new Merge("256");
    Tuple inputTuple = TupleFactory.getInstance().newTuple(1); //valid size, but null
    
    inputTuple.set(0, new Double(1.0)); //wrong type. Cannot Union datums.
    unionFunc.accumulate(inputTuple); //throws ClassCastException
  }
  
  @Test
  public void checkNullEmptyAccumulator() throws IOException {
    Accumulator<Tuple> unionFunc = new Merge("256");
    EvalFunc<Double> estFunc = new Estimate();
    
    Tuple inputTuple = null;
    unionFunc.accumulate(inputTuple); //does nothing, just returns
    
    inputTuple = TupleFactory.getInstance().newTuple(0); //invalid size
    unionFunc.accumulate(inputTuple); //does nothing, just returns
    
    inputTuple = TupleFactory.getInstance().newTuple(1); //valid size, but null
    unionFunc.accumulate(inputTuple); //does nothing, just returns
    
    inputTuple = TupleFactory.getInstance().newTuple(1); //valid size
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    inputTuple.set(0, bag); //correct type, but empty
    unionFunc.accumulate(inputTuple); //does nothing, just returns
    
    Tuple innerTuple = TupleFactory.getInstance().newTuple(0); //empty
    bag.add(innerTuple);
    unionFunc.accumulate(inputTuple); //does nothing, just returns
    
    inputTuple = TupleFactory.getInstance().newTuple(1); //valid size
    bag = BagFactory.getInstance().newDefaultBag();
    inputTuple.set(0, bag); //correct type
    innerTuple = TupleFactory.getInstance().newTuple(1); //correct size
    bag.add(innerTuple); //but innerTuple(0) is null
    unionFunc.accumulate(inputTuple); //ignores
    
    Tuple resultTuple = unionFunc.getValue();
    assertNotNull(resultTuple);
    assertEquals(resultTuple.size(), 1);
    
    Double est = estFunc.exec(resultTuple);
    assertEquals(est, 0.0, 0.0);
  }
  
  @Test
  public void checkEmptyGetValue() throws IOException {
    Accumulator<Tuple> unionFunc = new Merge("256");
    EvalFunc<Double> estFunc = new Estimate();
    
    Tuple resultTuple = unionFunc.getValue();
    DataByteArray dba = (DataByteArray) resultTuple.get(0);
    assertEquals(dba.size(), 8);
    Double est = estFunc.exec(resultTuple);
    assertEquals(est, 0.0, 0.0);
  }
  
  @Test
  public void checkExactAccumulator() throws IOException {
    Accumulator<Tuple> unionFunc = new Merge("256");
    EvalFunc<Double> estFunc = new Estimate();
    
    //create inputTuple and a bag, add bag to inputTuple
    Tuple inputTuple = TupleFactory.getInstance().newTuple(1);
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    inputTuple.set(0, bag);
    
    //create 4 distinct sketches of 32 in a bag
    for (int i = 0; i < 4; i++ ) {
      Tuple dataTuple = TupleFactory.getInstance().newTuple(1);
      dataTuple.set(0, createDbaFromQssRange(256, i*64, 64));
      
      bag.add(dataTuple);
    }
    
    unionFunc.accumulate(inputTuple); //A tuple, bag with 4 sketches
    
    Tuple resultTuple = unionFunc.getValue();
    assertNotNull(resultTuple);
    assertEquals(resultTuple.size(), 1);
    
    DataByteArray dba = (DataByteArray) resultTuple.get(0);
    assertTrue(dba.size() > 0);
    
    Double est = estFunc.exec(resultTuple);
    assertEquals(est, 256.0, 0.0);
    
    unionFunc.cleanup();
    
    resultTuple = unionFunc.getValue();
    assertNotNull(resultTuple);
    assertEquals(resultTuple.size(), 1);
    
    dba = (DataByteArray) resultTuple.get(0);
    assertTrue(dba.size() > 0);
    
    est = estFunc.exec(resultTuple);
    assertEquals(est, 0.0, 0.0);
  }
  
  @Test
  public void checkExactAlgebraicInitial() throws IOException {
    EvalFunc<Tuple> unionFuncInit = new Merge.Initial("256");
    
    Tuple inputTuple = TupleFactory.getInstance().newTuple(1);
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    inputTuple.set(0, bag);
    
    for (int i = 0; i < 4; i++ ) { //4 sketches with one value each
      Tuple dataTuple = TupleFactory.getInstance().newTuple(1);
      dataTuple.set(0, createDbaFromQssRange(16, i, 1));
      
      bag.add(dataTuple);
    }
    
    Tuple resultTuple = unionFuncInit.exec(inputTuple);
    assertTrue(resultTuple == inputTuple);  //returns the inputTuple
  }
  
  @Test
  public void checkAlgFinalFromPriorIntermed() throws IOException {
    EvalFunc<Tuple> unionFuncIFinal = new Merge.IntermediateFinal("256");
    EvalFunc<Double> estFunc = new Estimate();
    
    Tuple inputTuple = TupleFactory.getInstance().newTuple(1);
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    inputTuple.set(0, bag); //inputTuple.bag0:null
    
    for (int i = 0; i < 4; i++ ) {
      Tuple sketchTuple = TupleFactory.getInstance().newTuple(1);
      sketchTuple.set(0, createDbaFromQssRange(64, i*64, 64));
      
      bag.add(sketchTuple); 
      //inputTuple.bag0:sketchTuple0.DBA0
      //inputTuple.bag0:sketchTuple1.DBA1
      //inputTuple.bag0:sketchTuple2.DBA2
      //inputTuple.bag0:sketchTuple3.DBA3
    }
    
    Tuple resultTuple = unionFuncIFinal.exec(inputTuple);
    
    assertNotNull(resultTuple);
    assertEquals(resultTuple.size(), 1);
    
    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    assertTrue(bytes.size() > 0);
    
    Double est = estFunc.exec(resultTuple);
    assertEquals(est, 256.0, 0.0);
  }
  
  @Test
  public void checkAlgFinalFromPriorInitial() throws IOException {
    EvalFunc<Tuple> unionFuncFinal = new Merge.IntermediateFinal("256");
    EvalFunc<Double> estFunc = new Estimate();
    
    Tuple inputTuple = TupleFactory.getInstance().newTuple(1);
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    inputTuple.set(0, bag); //inputTuple.bag0:null
    
    Tuple innerTuple = TupleFactory.getInstance().newTuple(1);
    DataBag innerBag = BagFactory.getInstance().newDefaultBag();
    innerTuple.set(0, innerBag); //innerTuple.innerBag0:null
    bag.add(innerTuple); //inputTuple.bag0.innerTuple0.innerBag0:null
    
    for (int i = 0; i < 4; i++ ) {
      Tuple sketchTuple = TupleFactory.getInstance().newTuple(1);
      sketchTuple.set(0, createDbaFromQssRange(64, i*64, 64));
      
      innerBag.add(sketchTuple);
      //inputTuple.bag0.innerTuple0.innerBag0.sketchTuple0.DBA0
      //inputTuple.bag0.innerTuple0.innerBag0.sketchTuple1.DBA1
      //inputTuple.bag0.innerTuple0.innerBag0.sketchTuple2.DBA2
      //inputTuple.bag0.innerTuple0.innerBag0.sketchTuple3.DBA3
    }
    
    Tuple resultTuple = unionFuncFinal.exec(inputTuple);
    
    assertNotNull(resultTuple);
    assertEquals(resultTuple.size(), 1);
    
    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    assertTrue(bytes.size() > 0);
    
    Double est = estFunc.exec(resultTuple);
    assertEquals(est, 256.0, 0.0);
  }
  
  @Test
  public void checkAlgFinalOuterBagEmptyTuples() throws IOException {
    EvalFunc<Tuple> interFuncFinal = new Merge.IntermediateFinal("256");
    EvalFunc<Double> estFunc = new Estimate();
    
    Tuple inputTuple = TupleFactory.getInstance().newTuple(1);
    Tuple resultTuple = interFuncFinal.exec(inputTuple);
    assertEquals(estFunc.exec(resultTuple), 0.0, 0.0);
    
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    inputTuple.set(0, bag); //inputTuple.bag0:null
    resultTuple = interFuncFinal.exec(inputTuple);
    assertEquals(estFunc.exec(resultTuple), 0.0, 0.0);
    
    Tuple innerTuple = TupleFactory.getInstance().newTuple(1);
    bag.add(innerTuple);
    resultTuple = interFuncFinal.exec(inputTuple);
    assertEquals(estFunc.exec(resultTuple), 0.0, 0.0);
  }
  
  @Test
  public void checkAlgFinalInnerBagEmpty() throws IOException {
    EvalFunc<Tuple> interFuncFinal = new Merge.IntermediateFinal("256");
    EvalFunc<Double> estFunc = new Estimate();
    
    Tuple inputTuple = TupleFactory.getInstance().newTuple(1);
    Tuple resultTuple = interFuncFinal.exec(inputTuple);
    assertEquals(estFunc.exec(resultTuple), 0.0, 0.0);
    
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    inputTuple.set(0, bag); //inputTuple.bag0:null
    resultTuple = interFuncFinal.exec(inputTuple);
    assertEquals(estFunc.exec(resultTuple), 0.0, 0.0);
    
    Tuple innerTuple = TupleFactory.getInstance().newTuple(1);
    bag.add(innerTuple);
    DataBag bag2 = BagFactory.getInstance().newDefaultBag();
    innerTuple.set(0, bag2);
    
    resultTuple = interFuncFinal.exec(inputTuple);
    assertEquals(estFunc.exec(resultTuple), 0.0, 0.0);
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkAlgFinalInnerNotDBA() throws IOException {
    EvalFunc<Tuple> interFuncFinal = new Merge.IntermediateFinal("256");
    EvalFunc<Double> estFunc = new Estimate();
    
    Tuple inputTuple = TupleFactory.getInstance().newTuple(1);
    Tuple resultTuple = interFuncFinal.exec(inputTuple);
    assertEquals(estFunc.exec(resultTuple), 0.0, 0.0);
    
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    inputTuple.set(0, bag); //inputTuple.bag0:null
    resultTuple = interFuncFinal.exec(inputTuple);
    assertEquals(estFunc.exec(resultTuple), 0.0, 0.0);
    
    Tuple innerTuple = TupleFactory.getInstance().newTuple(1);
    bag.add(innerTuple);
    innerTuple.set(0, new Double(1.0)); //not a DBA
    
    resultTuple = interFuncFinal.exec(inputTuple);
    assertEquals(estFunc.exec(resultTuple), 0.0, 0.0);
  }
  
  @SuppressWarnings("null")
  @Test
  public void outputSchemaTest() throws IOException {
    EvalFunc<Tuple> udf = new Merge("512");
    
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
    println("Test");
  }
  
  /**
   * @param s value to print 
   */
  static void println(String s) {
    //System.out.println(s); //disable here
  }
  
}
