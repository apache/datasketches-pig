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

import static org.apache.datasketches.pig.PigTestingUtil.LS;
import static org.apache.datasketches.pig.PigTestingUtil.createDbaFromQssRange;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.IOException;

import org.apache.datasketches.SketchesStateException;
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

@SuppressWarnings("javadoc")
public class IntersectTest {

  @Test(expectedExceptions = IllegalStateException.class)
  public void checkGetValueExcep() {
    Intersect inter = new Intersect();
    inter.getValue();
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkNotDBAExcep() throws IOException {
    Intersect inter = new Intersect();
    //create inputTuple and a bag, add bag to inputTuple
    Tuple inputTuple = TupleFactory.getInstance().newTuple(1);
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    inputTuple.set(0, bag);

    Tuple innerTuple = TupleFactory.getInstance().newTuple(1);
    bag.add(innerTuple);
    inter.accumulate(inputTuple); //add empty tuple

    innerTuple.set(0, new Double(1.0));  //not a DBA
    inter = new Intersect();
    inter.accumulate(inputTuple); //add wrong type
  }

  @Test
  public void checkConstructors() {
    Intersect inter = new Intersect();
    inter = new Intersect(9001);

    Intersect.Initial initial = new Intersect.Initial();
    initial = new Intersect.Initial("9001");

    Intersect.IntermediateFinal interFin = new Intersect.IntermediateFinal();
    interFin = new Intersect.IntermediateFinal("9001");
    interFin = new Intersect.IntermediateFinal(9001);
    assertNotNull(initial);
    assertNotNull(interFin);
    inter.cleanup();
  }

  @Test
  public void checkNullInput() throws IOException {
    EvalFunc<Tuple> interFunc = new Intersect();
    EvalFunc<Double> estFunc = new Estimate();
    Tuple inputTuple = TupleFactory.getInstance().newTuple(1);
    //null bag

    Tuple resultTuple = interFunc.exec(inputTuple);
    assertNotNull(resultTuple);
    assertEquals(resultTuple.size(), 1);

    Double est = estFunc.exec(resultTuple);
    assertEquals(est, 0.0, 0.0);
  }

  @Test
  public void checkExactTopExec() throws IOException {
    EvalFunc<Tuple> interFunc = new Intersect();
    EvalFunc<Double> estFunc = new Estimate();

    //create inputTuple and a bag, add bag to inputTuple
    Tuple inputTuple = TupleFactory.getInstance().newTuple(1);
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    inputTuple.set(0, bag);

    //create 4 overlapping sketches of 64 in a bag
    for (int i = 0; i < 4; i++ ) {
      Tuple dataTuple = TupleFactory.getInstance().newTuple(1);
      dataTuple.set(0, createDbaFromQssRange(256, i*64, 256));

      bag.add(dataTuple);
    }

    Tuple resultTuple = interFunc.exec(inputTuple);
    assertNotNull(resultTuple);
    assertEquals(resultTuple.size(), 1);

    Double est = estFunc.exec(resultTuple);
    assertEquals(est, 64.0, 0.0);
  }

  @Test(expectedExceptions = ClassCastException.class)
  public void checkBadClassCast() throws IOException {
    Accumulator<Tuple> interFunc = new Intersect();
    Tuple inputTuple = TupleFactory.getInstance().newTuple(1); //valid size, but null

    inputTuple.set(0, new Double(1.0)); //wrong type. Cannot intersect datums.
    interFunc.accumulate(inputTuple); //throws ClassCastException
  }

  @Test
  public void checkNullEmptyAccumulator() throws IOException {
    Accumulator<Tuple> interFunc = new Intersect();
    EvalFunc<Double> estFunc = new Estimate();

    Tuple inputTuple = null;
    interFunc.accumulate(inputTuple); //does nothing

    inputTuple = TupleFactory.getInstance().newTuple(0); //invalid size
    interFunc.accumulate(inputTuple); //does nothing

    inputTuple = TupleFactory.getInstance().newTuple(1); //valid size, but null bag
    interFunc.accumulate(inputTuple); //does nothing

    inputTuple = TupleFactory.getInstance().newTuple(1); //valid size
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    inputTuple.set(0, bag); //correct type, but empty
    interFunc.accumulate(inputTuple); //does nothing

    Tuple innerTuple = TupleFactory.getInstance().newTuple(0); //empty
    bag.add(innerTuple);
    interFunc.accumulate(inputTuple); //does nothing

    inputTuple = TupleFactory.getInstance().newTuple(1); //valid size
    bag = BagFactory.getInstance().newDefaultBag();
    inputTuple.set(0, bag); //correct type
    innerTuple = TupleFactory.getInstance().newTuple(1); //correct size
    bag.add(innerTuple); //but innerTuple(0) is null
    interFunc.accumulate(inputTuple); //does nothing

    //Must call accumulate at least once before calling getValue.
    //To prove that all the above stuff truely did nothing,
    // we call accumulate once with a valid sketch and affirm that
    // getValue() returns it unaltered.

    //create inputTuple and a bag, add bag to inputTuple
    inputTuple = TupleFactory.getInstance().newTuple(1); //valid size
    bag = BagFactory.getInstance().newDefaultBag();
    inputTuple.set(0, bag);

    Tuple dataTuple = TupleFactory.getInstance().newTuple(1);
    dataTuple.set(0, createDbaFromQssRange(256, 0, 64));
    bag.add(dataTuple);

    interFunc.accumulate(inputTuple);

    Tuple resultTuple = interFunc.getValue();
    assertNotNull(resultTuple);
    assertEquals(resultTuple.size(), 1);

    Double est = estFunc.exec(resultTuple);
    assertEquals(est, 64.0, 0.0);
  }

  @Test
  public void checkExactAccumulator() throws IOException {
    Accumulator<Tuple> interFunc = new Intersect();
    EvalFunc<Double> estFunc = new Estimate();

    //create inputTuple and a bag, add bag to inputTuple
    Tuple inputTuple = TupleFactory.getInstance().newTuple(1);
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    inputTuple.set(0, bag);

    //create 4 distinct sketches of 32 in a bag
    for (int i = 0; i < 4; i++ ) {
      Tuple dataTuple = TupleFactory.getInstance().newTuple(1);
      dataTuple.set(0, createDbaFromQssRange(256, i*64, 256));

      bag.add(dataTuple);
    }

    interFunc.accumulate(inputTuple); //A tuple, bag with 4 sketches

    Tuple resultTuple = interFunc.getValue();
    assertNotNull(resultTuple);
    assertEquals(resultTuple.size(), 1);

    DataByteArray dba = (DataByteArray) resultTuple.get(0);
    assertTrue(dba.size() > 0);

    Double est = estFunc.exec(resultTuple);
    assertEquals(est, 64.0, 0.0);
  }

  @Test
  public void checkExactAlgebraicInitial() throws IOException {
    EvalFunc<Tuple> interFuncInit = new Intersect.Initial();

    Tuple inputTuple = TupleFactory.getInstance().newTuple(1);
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    inputTuple.set(0, bag);

    for (int i = 0; i < 4; i++ ) { //4 sketches with one value each
      Tuple dataTuple = TupleFactory.getInstance().newTuple(1);
      dataTuple.set(0, createDbaFromQssRange(16, i, 1));

      bag.add(dataTuple);
    }

    Tuple resultTuple = interFuncInit.exec(inputTuple);
    assertTrue(resultTuple == inputTuple);  //returns the inputTuple
  }

  @Test
  public void checkAlgFinalFromPriorIntermed() throws IOException {
    EvalFunc<Tuple> interFuncIFinal = new Intersect.IntermediateFinal();
    EvalFunc<Double> estFunc = new Estimate();

    Tuple inputTuple = TupleFactory.getInstance().newTuple(1);
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    inputTuple.set(0, bag); //inputTuple.bag0:null

    for (int i = 0; i < 4; i++ ) {
      Tuple sketchTuple = TupleFactory.getInstance().newTuple(1);
      sketchTuple.set(0, createDbaFromQssRange(256, i*64, 256));

      bag.add(sketchTuple);
      //inputTuple.bag0:sketchTuple0.DBA0
      //inputTuple.bag0:sketchTuple1.DBA1
      //inputTuple.bag0:sketchTuple2.DBA2
      //inputTuple.bag0:sketchTuple3.DBA3
    }

    Tuple resultTuple = interFuncIFinal.exec(inputTuple);

    assertNotNull(resultTuple);
    assertEquals(resultTuple.size(), 1);

    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    assertTrue(bytes.size() > 0);

    Double est = estFunc.exec(resultTuple);
    assertEquals(est, 64.0, 0.0);
  }

  @Test
  public void checkAlgFinalFromPriorInitial() throws IOException {
    EvalFunc<Tuple> interFuncFinal = new Intersect.IntermediateFinal();
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
      sketchTuple.set(0, createDbaFromQssRange(256, i*64, 256));

      innerBag.add(sketchTuple);
      //inputTuple.bag0.innerTuple0.innerBag0.sketchTuple0.DBA0
      //inputTuple.bag0.innerTuple0.innerBag0.sketchTuple1.DBA1
      //inputTuple.bag0.innerTuple0.innerBag0.sketchTuple2.DBA2
      //inputTuple.bag0.innerTuple0.innerBag0.sketchTuple3.DBA3
    }

    Tuple resultTuple = interFuncFinal.exec(inputTuple);

    assertNotNull(resultTuple);
    assertEquals(resultTuple.size(), 1);

    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    assertTrue(bytes.size() > 0);

    Double est = estFunc.exec(resultTuple);
    assertEquals(est, 64.0, 0.0);
  }

  @Test(expectedExceptions = SketchesStateException.class)
  public void checkAlgFinalOuterBagEmptyTuples() throws IOException {
    EvalFunc<Tuple> interFuncFinal = new Intersect.IntermediateFinal();
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
    resultTuple = interFuncFinal.exec(inputTuple); //Throws Illegal Result from HeapIntersection
    //assertEquals(estFunc.exec(resultTuple), 0.0, 0.0);
  }

  @Test(expectedExceptions = SketchesStateException.class)
  public void checkAlgFinalInnerBagEmpty() throws IOException {
    EvalFunc<Tuple> interFuncFinal = new Intersect.IntermediateFinal();
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

    resultTuple = interFuncFinal.exec(inputTuple); //Throws Illegal Result from HeapIntersection
    //assertEquals(estFunc.exec(resultTuple), 0.0, 0.0);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkAlgFinalInnerNotDBA() throws IOException {
    EvalFunc<Tuple> interFuncFinal = new Intersect.IntermediateFinal();
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

  @Test
  public void outputSchemaTest() throws IOException {
    EvalFunc<Tuple> udf = new Intersect();

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
