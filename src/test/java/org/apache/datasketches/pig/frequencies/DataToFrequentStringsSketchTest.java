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

package org.apache.datasketches.pig.frequencies;

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
import org.apache.datasketches.ArrayOfStringsSerDe;
import org.apache.datasketches.frequencies.ItemsSketch;
import org.apache.datasketches.pig.tuple.PigUtil;

@SuppressWarnings("javadoc")
public class DataToFrequentStringsSketchTest {

  @Test
  public void execNullInputTuple() throws Exception {
    EvalFunc<Tuple> func = new DataToFrequentStringsSketch("8");
    Tuple resultTuple = func.exec(null);
    Assert.assertNull(resultTuple);
  }

  @Test
  public void execEmptyInputTuple() throws Exception {
    EvalFunc<Tuple> func = new DataToFrequentStringsSketch("8");
    Tuple resultTuple = func.exec(TupleFactory.getInstance().newTuple());
    Assert.assertNull(resultTuple);
  }

  @Test
  public void execEmptyBag() throws Exception {
    EvalFunc<Tuple> func = new DataToFrequentStringsSketch("8");
    Tuple inputTuple = PigUtil.objectsToTuple(BagFactory.getInstance().newDefaultBag());
    Tuple resultTuple = func.exec(inputTuple);
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    ItemsSketch<String> sketch = ItemsSketch.getInstance(Memory.wrap(bytes.get()), new ArrayOfStringsSerDe());
    Assert.assertEquals(sketch.getNumActiveItems(), 0);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void execWrongSizeOfInnerTuple() throws Exception {
    EvalFunc<Tuple> func = new DataToFrequentStringsSketch("8");
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    bag.add(PigUtil.objectsToTuple());
    Tuple inputTuple = PigUtil.objectsToTuple(bag);
    func.exec(inputTuple);
  }

  @Test(expectedExceptions = ClassCastException.class)
  public void execWrongItemType() throws Exception {
    EvalFunc<Tuple> func = new DataToFrequentStringsSketch("8");
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    bag.add(PigUtil.objectsToTuple(new Object(), 1L)); // Object in place of String is not supported
    Tuple inputTuple = PigUtil.objectsToTuple(bag);
    func.exec(inputTuple);
  }

  @Test(expectedExceptions = ClassCastException.class)
  public void execWrongCountType() throws Exception {
    EvalFunc<Tuple> func = new DataToFrequentStringsSketch("8");
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    bag.add(PigUtil.objectsToTuple("a", 1)); // integer count is not supported
    Tuple inputTuple = PigUtil.objectsToTuple(bag);
    func.exec(inputTuple);
  }

  @Test
  public void exec() throws Exception {
    EvalFunc<Tuple> func = new DataToFrequentStringsSketch("8");
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    bag.add(PigUtil.objectsToTuple("a"));
    bag.add(PigUtil.objectsToTuple("b", 5L));
    bag.add(PigUtil.objectsToTuple("a", 2L));
    bag.add(PigUtil.objectsToTuple("b"));

    Tuple inputTuple = PigUtil.objectsToTuple(bag);
    Tuple resultTuple = func.exec(inputTuple);
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    ItemsSketch<String> sketch = ItemsSketch.getInstance(Memory.wrap(bytes.get()), new ArrayOfStringsSerDe());
    Assert.assertEquals(sketch.getNumActiveItems(), 2);
    Assert.assertEquals(sketch.getEstimate("a"), 3);
    Assert.assertEquals(sketch.getEstimate("b"), 6);
  }

  @Test
  public void accumulator() throws Exception {
    Accumulator<Tuple> func = new DataToFrequentStringsSketch("8");

    Tuple inputTuple = TupleFactory.getInstance().newTuple(1);
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    bag.add(PigUtil.objectsToTuple("a"));
    inputTuple.set(0, bag);

    func.accumulate(inputTuple);

    inputTuple = TupleFactory.getInstance().newTuple(1);
    bag = BagFactory.getInstance().newDefaultBag();
    bag.add(PigUtil.objectsToTuple("b"));
    bag.add(PigUtil.objectsToTuple("a", 2L));
    bag.add(PigUtil.objectsToTuple("b", 5L));
    inputTuple.set(0, bag);

    func.accumulate(inputTuple);

    Tuple resultTuple = func.getValue();
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    ItemsSketch<String> sketch = ItemsSketch.getInstance(Memory.wrap(bytes.get()), new ArrayOfStringsSerDe());
    Assert.assertEquals(sketch.getNumActiveItems(), 2);
    Assert.assertEquals(sketch.getEstimate("a"), 3);
    Assert.assertEquals(sketch.getEstimate("b"), 6);

    // after cleanup, the value should always be 0
    func.cleanup();
    resultTuple = func.getValue();
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    ItemsSketch<String> sketch2 = ItemsSketch.getInstance(Memory.wrap(bytes.get()), new ArrayOfStringsSerDe());
    Assert.assertTrue(sketch2.isEmpty());
    Assert.assertEquals(sketch2.getNumActiveItems(), 0);
  }

  @Test
  public void algebraicInitial() throws Exception {
    EvalFunc<Tuple> func = new DataToFrequentStringsSketch.Initial(null);
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

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void algebraicIntermediateFinalWrongType() throws Exception {
    EvalFunc<Tuple> func = new DataToFrequentStringsSketch.IntermediateFinal("8");
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    // this bag must have tuples with either bags or data byte arrays
    bag.add(TupleFactory.getInstance().newTuple(1.0));
    func.exec(TupleFactory.getInstance().newTuple(bag));
  }

  @Test
  public void algebraicIntermediateFinal() throws Exception {
    EvalFunc<Tuple> func = new DataToFrequentStringsSketch.IntermediateFinal("8");
    Tuple inputTuple = TupleFactory.getInstance().newTuple(1);
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    inputTuple.set(0, bag);

    // this is to simulate the output from Initial
    bag.add(PigUtil.objectsToTuple(PigUtil.tuplesToBag(PigUtil.objectsToTuple("a"))));

    // this is to simulate the output from a prior call of IntermediateFinal
    ItemsSketch<String> s = new ItemsSketch<>(8);
    s.update("b", 1L);
    s.update("a", 2L);
    s.update("b", 3L);
    bag.add(PigUtil.objectsToTuple(new DataByteArray(s.toByteArray(new ArrayOfStringsSerDe()))));

    Tuple resultTuple = func.exec(inputTuple);
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    ItemsSketch<String> sketch = ItemsSketch.getInstance(Memory.wrap(bytes.get()), new ArrayOfStringsSerDe());
    Assert.assertEquals(sketch.getNumActiveItems(), 2);
    Assert.assertEquals(sketch.getEstimate("a"), 3);
    Assert.assertEquals(sketch.getEstimate("b"), 4);
  }
}
