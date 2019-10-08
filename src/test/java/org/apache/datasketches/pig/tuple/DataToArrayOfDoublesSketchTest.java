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

package org.apache.datasketches.pig.tuple;

import org.apache.pig.Accumulator;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.tuple.ArrayOfDoublesSketch;
import org.apache.datasketches.tuple.ArrayOfDoublesSketches;
import org.apache.datasketches.tuple.ArrayOfDoublesUpdatableSketch;
import org.apache.datasketches.tuple.ArrayOfDoublesUpdatableSketchBuilder;

@SuppressWarnings("javadoc")
public class DataToArrayOfDoublesSketchTest {

  @Test
  public void execNullInputTuple() throws Exception {
    EvalFunc<Tuple> func = new DataToArrayOfDoublesSketch();
    Tuple resultTuple = func.exec(null);
    Assert.assertNull(resultTuple);
  }

  @Test
  public void execEmptyInputTuple() throws Exception {
    EvalFunc<Tuple> func = new DataToArrayOfDoublesSketch();
    Tuple resultTuple = func.exec(TupleFactory.getInstance().newTuple());
    Assert.assertNull(resultTuple);
  }

  @Test
  public void execEmptyBag() throws Exception {
    EvalFunc<Tuple> func = new DataToArrayOfDoublesSketch("1");
    Tuple inputTuple = PigUtil.objectsToTuple(BagFactory.getInstance().newDefaultBag());
    Tuple resultTuple = func.exec(inputTuple);
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    ArrayOfDoublesSketch sketch = ArrayOfDoublesSketches.heapifySketch(Memory.wrap(bytes.get()));
    Assert.assertEquals(sketch.getEstimate(), 0.0);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void execWrongSizeOfInnerTuple() throws Exception {
    EvalFunc<Tuple> func = new DataToArrayOfDoublesSketch("32", "1");
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    bag.add(PigUtil.objectsToTuple(1));
    Tuple inputTuple = PigUtil.objectsToTuple(bag);
    func.exec(inputTuple);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void execWrongKeyType() throws Exception {
    EvalFunc<Tuple> func = new DataToArrayOfDoublesSketch("32", "1");
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    bag.add(PigUtil.objectsToTuple(new Object(), 1.0)); // Object in place of key is not supported
    Tuple inputTuple = PigUtil.objectsToTuple(bag);
    func.exec(inputTuple);
  }

  @Test
  public void execAllInputTypes() throws Exception {
    EvalFunc<Tuple> func = new DataToArrayOfDoublesSketch("32", "1");
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    bag.add(PigUtil.objectsToTuple("a", 1.0));
    bag.add(PigUtil.objectsToTuple("b", 1.0));
    bag.add(PigUtil.objectsToTuple("a", 2.0));
    bag.add(PigUtil.objectsToTuple("b", 2.0));

    bag.add(PigUtil.objectsToTuple(1, 3.0));
    bag.add(PigUtil.objectsToTuple(2L, 3.0));
    bag.add(PigUtil.objectsToTuple(1f, 3.0));
    bag.add(PigUtil.objectsToTuple(2.0, 3.0));
    bag.add(PigUtil.objectsToTuple((byte) 3, 3.0));
    bag.add(PigUtil.objectsToTuple(new DataByteArray("c".getBytes()), 3.0));

    Tuple inputTuple = PigUtil.objectsToTuple(bag);
    Tuple resultTuple = func.exec(inputTuple);
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    ArrayOfDoublesSketch sketch = ArrayOfDoublesSketches.heapifySketch(Memory.wrap(bytes.get()));
    Assert.assertEquals(sketch.getEstimate(), 8.0, 0.0);

    for (double[] values: sketch.getValues()) {
      Assert.assertEquals(values[0], 3.0);
    }
  }

  @Test
  public void execWithSampling() throws Exception {
    EvalFunc<Tuple> func = new DataToArrayOfDoublesSketch("1024", "0.5", "1");
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    int uniques = 10000;
    for (int i = 0; i < uniques; i++) {
      bag.add(PigUtil.objectsToTuple(i, 1.0));
    }
    Tuple resultTuple = func.exec(PigUtil.objectsToTuple(bag));
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    ArrayOfDoublesSketch sketch = ArrayOfDoublesSketches.heapifySketch(Memory.wrap(bytes.get()));
    Assert.assertEquals(sketch.getEstimate(), uniques, uniques * 0.01);
  }

  @Test
  public void accumulator() throws Exception {
    Accumulator<Tuple> func = new DataToArrayOfDoublesSketch("32", "1");

    Tuple inputTuple = TupleFactory.getInstance().newTuple(1);
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    bag.add(PigUtil.objectsToTuple("a", 1.0));
    inputTuple.set(0, bag);

    func.accumulate(inputTuple);

    inputTuple = TupleFactory.getInstance().newTuple(1);
    bag = BagFactory.getInstance().newDefaultBag();
    bag.add(PigUtil.objectsToTuple("b", 1.0));
    bag.add(PigUtil.objectsToTuple("a", 2.0));
    bag.add(PigUtil.objectsToTuple("b", 2.0));
    inputTuple.set(0, bag);

    func.accumulate(inputTuple);

    Tuple resultTuple = func.getValue();
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    ArrayOfDoublesSketch sketch = ArrayOfDoublesSketches.heapifySketch(Memory.wrap(bytes.get()));
    Assert.assertEquals(sketch.getEstimate(), 2.0, 0.0);

    for (double[] values: sketch.getValues()) {
      Assert.assertEquals(values[0], 3.0);
    }

    // after cleanup, the value should always be 0
    func.cleanup();
    resultTuple = func.getValue();
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    ArrayOfDoublesSketch sketch2 = ArrayOfDoublesSketches.heapifySketch(Memory.wrap(bytes.get()));
    Assert.assertEquals(sketch2.getEstimate(), 0.0, 0.0);
  }

  @Test
  public void algebraicInitial() throws Exception {
    EvalFunc<Tuple> func = new DataToArrayOfDoublesSketch.Initial(null);
    Tuple inputTuple = TupleFactory.getInstance().newTuple(1);
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    bag.add(PigUtil.objectsToTuple(null, null));
    bag.add(PigUtil.objectsToTuple(null, null));
    bag.add(PigUtil.objectsToTuple(null, null));
    inputTuple.set(0, bag);

    Tuple resultTuple = func.exec(inputTuple);
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataBag resultBag = (DataBag) resultTuple.get(0);
    Assert.assertEquals(resultBag.size(), 3);
  }

  @Test
  public void algebraicIntermediateFinal() throws Exception {
    EvalFunc<Tuple> func = new DataToArrayOfDoublesSketch.IntermediateFinal("1");
    Tuple inputTuple = TupleFactory.getInstance().newTuple(1);
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    inputTuple.set(0, bag);

    // this is to simulate the output from Initial
    bag.add(PigUtil.objectsToTuple(PigUtil.tuplesToBag(PigUtil.objectsToTuple("a", 1.0))));

    // this is to simulate the output from a prior call of IntermediateFinal
    {
      ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().build();
      sketch.update("b", new double[] {1.0});
      sketch.update("a", new double[] {2.0});
      sketch.update("b", new double[] {2.0});
      bag.add(PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray())));
    }

    Tuple resultTuple = func.exec(inputTuple);
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    ArrayOfDoublesSketch sketch = ArrayOfDoublesSketches.heapifySketch(Memory.wrap(bytes.get()));
    Assert.assertEquals(sketch.getEstimate(), 2.0, 0.0);

    for (double[] values: sketch.getValues()) {
      Assert.assertEquals(values[0], 3.0);
    }
  }

  @Test
  public void algebraicIntermediateFinalWithSampling() throws Exception {
    EvalFunc<Tuple> func = new DataToArrayOfDoublesSketch.IntermediateFinal("1024", "0.5", "1");

    DataBag bag = BagFactory.getInstance().newDefaultBag();
    int uniques = 10000;
    for (int i = 0; i < uniques; i++) {
      bag.add(PigUtil.objectsToTuple(i, 1.0));
    }

    Tuple resultTuple = func.exec(PigUtil.objectsToTuple(PigUtil.tuplesToBag(PigUtil.objectsToTuple(bag))));
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    ArrayOfDoublesSketch sketch = ArrayOfDoublesSketches.heapifySketch(Memory.wrap(bytes.get()));
    Assert.assertEquals(sketch.getEstimate(), uniques, uniques * 0.01);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void algebraicIntermediateFinalNullBag() throws Exception {
    EvalFunc<Tuple> func = new DataToArrayOfDoublesSketch.IntermediateFinal("32", "1");
    func.exec(TupleFactory.getInstance().newTuple(1));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void algebraicIntermediateFinalWrongType() throws Exception {
    EvalFunc<Tuple> func = new DataToArrayOfDoublesSketch.IntermediateFinal("32", "1");
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    bag.add(TupleFactory.getInstance().newTuple(1.0));
    func.exec(TupleFactory.getInstance().newTuple(bag));
  }
}
