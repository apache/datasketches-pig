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

import java.util.Random;

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
public class UnionArrayOfDoublesSketchTest {

  @Test
  public void execNullInput() throws Exception {
    EvalFunc<Tuple> func = new UnionArrayOfDoublesSketch("32", "1");
    Tuple resultTuple = func.exec(null);
    Assert.assertNull(resultTuple);
  }

  @Test
  public void execEmptyInputTuple() throws Exception {
    EvalFunc<Tuple> func = new UnionArrayOfDoublesSketch("32", "1");
    Tuple resultTuple = func.exec(TupleFactory.getInstance().newTuple());
    Assert.assertNull(resultTuple);
  }

  @Test
  public void exec() throws Exception {
    EvalFunc<Tuple> func = new UnionArrayOfDoublesSketch("4096", "1");
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    {
      ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().build();
      sketch.update(1, new double[] {1.0});
      sketch.update(2, new double[] {1.0});
      bag.add(PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray())));
    }
    {
      ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().build();
      sketch.update(1, new double[] {1.0});
      sketch.update(2, new double[] {1.0});
      bag.add(PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray())));
    }
    Tuple resultTuple = func.exec(PigUtil.objectsToTuple(bag));
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    ArrayOfDoublesSketch sketch = ArrayOfDoublesSketches.heapifySketch(Memory.wrap(bytes.get()));
    Assert.assertEquals(sketch.getEstimate(), 2.0, 0.0);
    for (double[] values: sketch.getValues()) {
      Assert.assertEquals(values[0], 2.0, 0.0);
    }
  }

  @Test
  public void accumulatorNullInput() throws Exception {
    Accumulator<Tuple> func = new UnionArrayOfDoublesSketch();
    func.accumulate(null);
    Tuple resultTuple = func.getValue();
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    ArrayOfDoublesSketch sketch = ArrayOfDoublesSketches.heapifySketch(Memory.wrap(bytes.get()));
    Assert.assertEquals(sketch.getEstimate(), 0.0);
  }

  @Test
  public void accumulatorEmptyInputTuple() throws Exception {
    Accumulator<Tuple> func = new UnionArrayOfDoublesSketch();
    func.accumulate(TupleFactory.getInstance().newTuple());
    Tuple resultTuple = func.getValue();
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    ArrayOfDoublesSketch sketch = ArrayOfDoublesSketches.heapifySketch(Memory.wrap(bytes.get()));
    Assert.assertEquals(sketch.getEstimate(), 0.0);
  }

  @Test
  public void accumulatorNotABag() throws Exception {
    Accumulator<Tuple> func = new UnionArrayOfDoublesSketch();
    func.accumulate(PigUtil.objectsToTuple((Object) null));
    Tuple resultTuple = func.getValue();
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    ArrayOfDoublesSketch sketch = ArrayOfDoublesSketches.heapifySketch(Memory.wrap(bytes.get()));
    Assert.assertEquals(sketch.getEstimate(), 0.0);
  }

  @Test
  public void accumulatorEmptyBag() throws Exception {
    Accumulator<Tuple> func = new UnionArrayOfDoublesSketch();
    func.accumulate(PigUtil.objectsToTuple(BagFactory.getInstance().newDefaultBag()));
    Tuple resultTuple = func.getValue();
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    ArrayOfDoublesSketch sketch = ArrayOfDoublesSketches.heapifySketch(Memory.wrap(bytes.get()));
    Assert.assertEquals(sketch.getEstimate(), 0.0);
  }

  @Test
  public void accumulatorEmptyInnerTuple() throws Exception {
    Accumulator<Tuple> func = new UnionArrayOfDoublesSketch();
    func.accumulate(PigUtil.objectsToTuple(PigUtil.tuplesToBag(TupleFactory.getInstance().newTuple())));
    Tuple resultTuple = func.getValue();
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    ArrayOfDoublesSketch sketch = ArrayOfDoublesSketches.heapifySketch(Memory.wrap(bytes.get()));
    Assert.assertEquals(sketch.getEstimate(), 0.0);
  }

  @Test
  public void accumulatorNullSketch() throws Exception {
    Accumulator<Tuple> func = new UnionArrayOfDoublesSketch();
    func.accumulate(PigUtil.objectsToTuple(PigUtil.tuplesToBag(PigUtil.objectsToTuple((Object) null))));
    Tuple resultTuple = func.getValue();
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    ArrayOfDoublesSketch sketch = ArrayOfDoublesSketches.heapifySketch(Memory.wrap(bytes.get()));
    Assert.assertEquals(sketch.getEstimate(), 0.0);
  }

  @Test
  public void accumulatorEmptySketch() throws Exception {
    Accumulator<Tuple> func = new UnionArrayOfDoublesSketch("1");
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    {
      ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().build();
      bag.add(PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray())));
    }
    func.accumulate(PigUtil.objectsToTuple(bag));
    Tuple resultTuple = func.getValue();
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    ArrayOfDoublesSketch sketch = ArrayOfDoublesSketches.heapifySketch(Memory.wrap(bytes.get()));
    Assert.assertEquals(sketch.getEstimate(), 0.0);
  }

  @Test
  public void accumulator() throws Exception {
    Accumulator<Tuple> func = new UnionArrayOfDoublesSketch("4096", "1");
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    {
      ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().build();
      sketch.update(1, new double[] {1.0});
      sketch.update(2, new double[] {1.0});
      bag.add(PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray())));
    }
    func.accumulate(PigUtil.objectsToTuple(bag));

    bag = BagFactory.getInstance().newDefaultBag();
    {
      ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().build();
      sketch.update(1, new double[] {1.0});
      sketch.update(2, new double[] {1.0});
      bag.add(PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray())));
    }
    func.accumulate(PigUtil.objectsToTuple(bag));

    Tuple resultTuple = func.getValue();
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    ArrayOfDoublesSketch sketch = ArrayOfDoublesSketches.heapifySketch(Memory.wrap(bytes.get()));
    Assert.assertEquals(sketch.getEstimate(), 2.0, 0.0);
    for (double[] values: sketch.getValues()) {
      Assert.assertEquals(values[0], 2.0, 0.0);
    }
  }

  @Test
  public void algebraicInitial() throws Exception {
    EvalFunc<Tuple> func = new UnionArrayOfDoublesSketch.Initial(null);
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    bag.add(null);
    bag.add(null);
    bag.add(null);

    Tuple resultTuple = func.exec(PigUtil.objectsToTuple(bag));
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataBag resultBag = (DataBag) resultTuple.get(0);
    Assert.assertEquals(resultBag.size(), 3);
  }

  @Test
  public void algebraicIntemediateFinalExact() throws Exception {
    EvalFunc<Tuple> func = new UnionArrayOfDoublesSketch.IntermediateFinal("1");
    DataBag bag = BagFactory.getInstance().newDefaultBag();

    // this is to simulate the output from Initial
    {
      ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().build();
      sketch.update(1, new double[] {1.0});
      sketch.update(2, new double[] {1.0});
      DataBag innerBag = PigUtil.tuplesToBag(PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray())));
      bag.add(PigUtil.objectsToTuple(innerBag));
    }

    // this is to simulate the output from a prior call of IntermediateFinal
    {
      ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().build();
      sketch.update(1, new double[] {1.0});
      sketch.update(2, new double[] {1.0});
      bag.add(PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray())));
    }

    Tuple resultTuple = func.exec(PigUtil.objectsToTuple(bag));
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    ArrayOfDoublesSketch sketch = ArrayOfDoublesSketches.heapifySketch(Memory.wrap(bytes.get()));
    Assert.assertEquals(sketch.getEstimate(), 2.0, 0.0);
    for (double[] values: sketch.getValues()) {
      Assert.assertEquals(values[0], 2.0, 0.0);
    }
  }

  @Test
  public void algebraicIntemediateFinalEstimation() throws Exception {
    EvalFunc<Tuple> func = new UnionArrayOfDoublesSketch.IntermediateFinal("16384", "1");
    DataBag bag = BagFactory.getInstance().newDefaultBag();

    long value = 1;
    // this is to simulate the output from Initial
    {
      ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().setNominalEntries(16384).build();
      for (int i = 0; i < 20000; i++) {
        sketch.update(value++, new double[] {1.0});
      }
      DataBag innerBag = PigUtil.tuplesToBag(PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray())));
      bag.add(PigUtil.objectsToTuple(innerBag));
    }

    // this is to simulate the output from a prior call of IntermediateFinal
    {
      ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().setNominalEntries(16384).build();
      for (int i = 0; i < 20000; i++) {
        sketch.update(value++, new double[] {1.0});
      }
      bag.add(PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray())));
    }

    Tuple resultTuple = func.exec(PigUtil.objectsToTuple(bag));
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    ArrayOfDoublesSketch sketch = ArrayOfDoublesSketches.heapifySketch(Memory.wrap(bytes.get()));
    Assert.assertEquals(sketch.getEstimate(), 40000.0, 40000.0 * 0.01);
    for (double[] values: sketch.getValues()) {
      Assert.assertEquals(values[0], 1.0, 0.0);
    }
  }

  @Test
  public void algebraicIntemediateFinalSingleCall() throws Exception {
    EvalFunc<Tuple> func = new UnionArrayOfDoublesSketch.IntermediateFinal("1024", "1");
    DataBag bag = BagFactory.getInstance().newDefaultBag();

    long value = 1;
    // this is to simulate the output from a prior call of IntermediateFinal
    {
      ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().setNominalEntries(1024).build();
      for (int i = 0; i < 10000; i++) {
        sketch.update(value++, new double[] {1.0});
      }
      bag.add(PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray())));
    }

    Tuple resultTuple = func.exec(PigUtil.objectsToTuple(bag));
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    ArrayOfDoublesSketch sketch = ArrayOfDoublesSketches.heapifySketch(Memory.wrap(bytes.get()));
    Assert.assertEquals(sketch.getEstimate(), 10000.0, 10000.0 * 0.02);
    for (double[] values: sketch.getValues()) {
      Assert.assertEquals(values[0], 1.0, 0.0);
    }
  }

  @Test
  public void algebraicIntemediateFinalRandomized() throws Exception {
    EvalFunc<Tuple> func = new UnionArrayOfDoublesSketch.IntermediateFinal("16384", "1");
    DataBag bag = BagFactory.getInstance().newDefaultBag();

    long key = 1;
    Random rnd = new Random();
    long uniques = 0;
    long updates = 0;
    // this is to simulate the output from a prior call of IntermediateFinal
    {
      ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().setNominalEntries(16384).build();
      for (int i = 0; i < 40000; i++) {
        sketch.update(key++, new double[] {rnd.nextDouble() * 20});
      }
      bag.add(PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray())));
      uniques += 40000;
      updates += 40000;
    }
    key -= 20000; // overlap
    {
      ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().setNominalEntries(16384).build();
      for (int i = 0; i < 60000; i++) {
        sketch.update(key++, new double[] {rnd.nextDouble() * 20});
      }
      bag.add(PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray())));
      uniques += 40000;
      updates += 60000;
    }
    key -= 20000; // overlap
    {
      ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().setNominalEntries(16384).build();
      for (int i = 0; i < 60000; i++) {
        sketch.update(key++, new double[] {rnd.nextDouble() * 20});
      }
      bag.add(PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray())));
      uniques += 40000;
      updates += 60000;
    }
    key -= 20000; // overlap
    {
      ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().setNominalEntries(16384).build();
      for (int i = 0; i < 60000; i++) {
        sketch.update(key++, new double[] {rnd.nextDouble() * 20});
      }
      bag.add(PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray())));
      uniques += 40000;
      updates += 60000;
    }
    {
      ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().setNominalEntries(16384).build();
      for (int i = 0; i < 40000; i++) {
        sketch.update(key++, new double[] {rnd.nextDouble() * 20});
      }
      bag.add(PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray())));
      uniques += 40000;
      updates += 40000;
    }
    {
      ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().setNominalEntries(16384).build();
      for (int i = 0; i < 40000; i++) {
        sketch.update(key++, new double[] {rnd.nextDouble() * 20});
      }
      bag.add(PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray())));
      uniques += 40000;
      updates += 40000;
    }
    key -= 20000; // overlap
    {
      ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().setNominalEntries(16384).build();
      for (int i = 0; i < 60000; i++) {
        sketch.update(key++, new double[] {rnd.nextDouble() * 20});
      }
      bag.add(PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray())));
      uniques += 40000;
      updates += 60000;
    }

    Tuple resultTuple = func.exec(PigUtil.objectsToTuple(bag));
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    DataByteArray bytes = (DataByteArray) resultTuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    ArrayOfDoublesSketch sketch = ArrayOfDoublesSketches.heapifySketch(Memory.wrap(bytes.get()));
    Assert.assertEquals(sketch.getEstimate(), uniques, uniques * 0.01);
    double sum = 0;
    for (double[] values: sketch.getValues()) {
      sum += values[0];
    }
    // each update added 10 to the total on average
    Assert.assertEquals(sum / sketch.getTheta(), updates * 10.0, updates * 10.0 * 0.02); // there is a slight chance of failing here
  }
}
