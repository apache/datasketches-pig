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

package org.apache.datasketches.pig.kll;

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
import org.apache.datasketches.kll.KllFloatsSketch;

@SuppressWarnings("javadoc")
public class DataToSketchTest {
  private static final TupleFactory TUPLE_FACTORY = TupleFactory.getInstance();
  private static final BagFactory BAG_FACTORY = BagFactory.getInstance();

  @Test
  public void execNullInputTuple() throws Exception {
    final EvalFunc<DataByteArray> func = new DataToSketch();
    final DataByteArray result = func.exec(null);
    final KllFloatsSketch sketch = getSketch(result);
    Assert.assertTrue(sketch.isEmpty());
  }

  @Test
  public void execEmptyInputTuple() throws Exception {
    final EvalFunc<DataByteArray> func = new DataToSketch();
    final DataByteArray result = func.exec(TUPLE_FACTORY.newTuple());
    final KllFloatsSketch sketch = getSketch(result);
    Assert.assertTrue(sketch.isEmpty());
  }

  @Test
  public void execEmptyBag() throws Exception {
    final EvalFunc<DataByteArray> func = new DataToSketch();
    final DataByteArray result = func.exec(TUPLE_FACTORY.newTuple(BAG_FACTORY.newDefaultBag()));
    final KllFloatsSketch sketch = getSketch(result);
    Assert.assertTrue(sketch.isEmpty());
  }

  @Test(expectedExceptions = ClassCastException.class)
  public void execWrongValueType() throws Exception {
    final EvalFunc<DataByteArray> func = new DataToSketch();
    final DataBag bag = BAG_FACTORY.newDefaultBag();
    bag.add(TUPLE_FACTORY.newTuple("a"));
    func.exec(TUPLE_FACTORY.newTuple(bag));
  }

  @Test
  public void execNormalCase() throws Exception {
    final EvalFunc<DataByteArray> func = new DataToSketch();
    final DataBag bag = BAG_FACTORY.newDefaultBag();
    bag.add(TUPLE_FACTORY.newTuple(Float.valueOf(1)));
    final DataByteArray result = func.exec(TUPLE_FACTORY.newTuple(bag));
    final KllFloatsSketch sketch = getSketch(result);
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertEquals(sketch.getN(), 1);
  }

  @Test
  public void execMixedNullCase() throws Exception {
    final EvalFunc<DataByteArray> func = new DataToSketch();
    final DataBag bag = BAG_FACTORY.newDefaultBag();
    bag.add(TUPLE_FACTORY.newTuple(Float.valueOf(1)));
    bag.add(null);
    final DataByteArray result = func.exec(TUPLE_FACTORY.newTuple(bag));
    final KllFloatsSketch sketch = getSketch(result);
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertEquals(sketch.getN(), 1);
  }

  @Test
  public void accumulator() throws Exception {
    final Accumulator<DataByteArray> func = new DataToSketch();

    // no input yet
    DataByteArray result = func.getValue();
    KllFloatsSketch sketch = getSketch(result);
    Assert.assertTrue(sketch.isEmpty());

    // null input tuple
    func.accumulate(null);
    result = func.getValue();
    sketch = getSketch(result);
    Assert.assertTrue(sketch.isEmpty());

    // empty input tuple
    func.accumulate(TUPLE_FACTORY.newTuple());
    result = func.getValue();
    sketch = getSketch(result);
    Assert.assertTrue(sketch.isEmpty());

    // empty bag
    func.accumulate(TUPLE_FACTORY.newTuple(BAG_FACTORY.newDefaultBag()));
    result = func.getValue();
    sketch = getSketch(result);
    Assert.assertTrue(sketch.isEmpty());

    // normal case
    DataBag bag = BAG_FACTORY.newDefaultBag();
    bag.add(TUPLE_FACTORY.newTuple(Float.valueOf(1)));
    func.accumulate(TUPLE_FACTORY.newTuple(bag));
    func.accumulate(TUPLE_FACTORY.newTuple(bag));
    result = func.getValue();
    sketch = getSketch(result);
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertEquals(sketch.getN(), 2);

    // mixed null case
    bag = BAG_FACTORY.newDefaultBag();
    bag.add(TUPLE_FACTORY.newTuple(Float.valueOf(1)));
    bag.add(null);
    func.accumulate(TUPLE_FACTORY.newTuple(bag));
    func.accumulate(TUPLE_FACTORY.newTuple(bag));
    result = func.getValue();
    sketch = getSketch(result);
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertEquals(sketch.getN(), 4);

    // cleanup
    func.cleanup();
    result = func.getValue();
    sketch = getSketch(result);
    Assert.assertTrue(sketch.isEmpty());
  }

  @Test
  public void accumulatorCustomK() throws Exception {
    final Accumulator<DataByteArray> func = new DataToSketch("400");
    final KllFloatsSketch sketch = getSketch(func.getValue());
    Assert.assertEquals(sketch.getK(), 400);
  }

  @Test
  public void algebraicInitial() throws Exception {
    final EvalFunc<Tuple> func = new DataToSketch.Initial();
    final DataBag bag = BAG_FACTORY.newDefaultBag();
    bag.add(TUPLE_FACTORY.newTuple());
    final Tuple resultTuple = func.exec(TUPLE_FACTORY.newTuple(bag));
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    Assert.assertTrue(resultTuple.get(0) instanceof DataBag);
    Assert.assertEquals(((DataBag) resultTuple.get(0)).size(), 1);
  }

  @Test
  public void algebraicIntermediateNullInputTupleCustomK() throws Exception {
    @SuppressWarnings("unchecked")
    final EvalFunc<Tuple> func = (EvalFunc<Tuple>) Class.forName(new DataToSketch().getIntermed())
        .getConstructor(String.class).newInstance("400");
    final Tuple resultTuple = func.exec(null);
    final KllFloatsSketch sketch = getSketch(resultTuple);
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertEquals(sketch.getK(), 400);
  }

  @Test
  public void algebraicIntermediateEmptyInputTuple() throws Exception {
    @SuppressWarnings("unchecked")
    final EvalFunc<Tuple> func = (EvalFunc<Tuple>) Class.forName(new DataToSketch().getIntermed()).newInstance();
    final Tuple resultTuple = func.exec(TUPLE_FACTORY.newTuple());
    final KllFloatsSketch sketch = getSketch(resultTuple);
    Assert.assertTrue(sketch.isEmpty());
  }

  @Test
  public void algebraicIntermediateNormalCase() throws Exception {
    @SuppressWarnings("unchecked")
    final EvalFunc<Tuple> func = (EvalFunc<Tuple>) Class.forName(new DataToSketch().getIntermed()).newInstance();
    final DataBag bag = BAG_FACTORY.newDefaultBag();

    { // this is to simulate an output from Initial
      final DataBag innerBag = BAG_FACTORY.newDefaultBag();
      innerBag.add(TUPLE_FACTORY.newTuple(Float.valueOf(1)));
      bag.add(TUPLE_FACTORY.newTuple(innerBag));
    }

    { // this is to simulate an output from a prior call of IntermediateFinal
      final KllFloatsSketch qs = new KllFloatsSketch();
      qs.update(2);
      bag.add(TUPLE_FACTORY.newTuple(new DataByteArray(qs.toByteArray())));
    }

    final Tuple resultTuple = func.exec(TUPLE_FACTORY.newTuple(bag));
    final KllFloatsSketch sketch = getSketch(resultTuple);
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertEquals(sketch.getN(), 2);
  }

  @Test
  public void algebraicIntermediateMixedNullCase() throws Exception {
    @SuppressWarnings("unchecked")
    final EvalFunc<Tuple> func = (EvalFunc<Tuple>) Class.forName(new DataToSketch().getIntermed()).newInstance();
    final DataBag bag = BAG_FACTORY.newDefaultBag();

    { // this is to simulate an output from Initial
      final DataBag innerBag = BAG_FACTORY.newDefaultBag();
      innerBag.add(TUPLE_FACTORY.newTuple(Float.valueOf(1)));
      innerBag.add(null);
      bag.add(TUPLE_FACTORY.newTuple(innerBag));
    }

    { // this is to simulate an output from a prior call of IntermediateFinal
      final KllFloatsSketch qs = new KllFloatsSketch();
      qs.update(2);
      bag.add(TUPLE_FACTORY.newTuple(new DataByteArray(qs.toByteArray())));
    }

    final Tuple resultTuple = func.exec(TUPLE_FACTORY.newTuple(bag));
    final KllFloatsSketch sketch = getSketch(resultTuple);
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertEquals(sketch.getN(), 2);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void algebraicIntermediateWrongType() throws Exception {
    @SuppressWarnings("unchecked")
    final EvalFunc<Tuple> func = (EvalFunc<Tuple>) Class.forName(new DataToSketch().getIntermed()).newInstance();
    final DataBag bag = BAG_FACTORY.newDefaultBag();

    // this bag must have tuples with either bags or data byte arrays
    bag.add(TUPLE_FACTORY.newTuple(1.0));
    func.exec(TUPLE_FACTORY.newTuple(bag));
  }

  @Test
  public void algebraicFinalNullInputTupleCustomK() throws Exception {
    @SuppressWarnings("unchecked")
    final EvalFunc<DataByteArray> func = (EvalFunc<DataByteArray>) Class.forName(new DataToSketch().getFinal())
        .getConstructor(String.class).newInstance("400");
    final DataByteArray result = func.exec(null);
    final KllFloatsSketch sketch = getSketch(result);
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertEquals(sketch.getK(), 400);
  }

  @Test
  public void algebraicFinalEmptyInputTuple() throws Exception {
    @SuppressWarnings("unchecked")
    final EvalFunc<DataByteArray> func = (EvalFunc<DataByteArray>) Class.forName(new DataToSketch().getFinal()).newInstance();
    final DataByteArray result = func.exec(TUPLE_FACTORY.newTuple());
    final KllFloatsSketch sketch = getSketch(result);
    Assert.assertTrue(sketch.isEmpty());
  }

  @Test
  public void algebraicFinalNormalCase() throws Exception {
    @SuppressWarnings("unchecked")
    final EvalFunc<DataByteArray> func = (EvalFunc<DataByteArray>) Class.forName(new DataToSketch().getFinal()).newInstance();
    final DataBag bag = BAG_FACTORY.newDefaultBag();

    { // this is to simulate an output from Initial
      final DataBag innerBag = BAG_FACTORY.newDefaultBag();
      innerBag.add(TUPLE_FACTORY.newTuple(Float.valueOf(1)));
      bag.add(TUPLE_FACTORY.newTuple(innerBag));
    }

    { // this is to simulate an output from a prior call of Intermediate
      final KllFloatsSketch qs = new KllFloatsSketch();
      qs.update(2);
      bag.add(TUPLE_FACTORY.newTuple(new DataByteArray(qs.toByteArray())));
    }

    final DataByteArray result = func.exec(TUPLE_FACTORY.newTuple(bag));
    final KllFloatsSketch sketch = getSketch(result);
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertEquals(sketch.getN(), 2);
  }

  @Test
  public void algebraicFinalMixedNullCase() throws Exception {
    @SuppressWarnings("unchecked")
    final EvalFunc<DataByteArray> func = (EvalFunc<DataByteArray>) Class.forName(new DataToSketch().getFinal()).newInstance();
    final DataBag bag = BAG_FACTORY.newDefaultBag();

    { // this is to simulate an output from Initial
      final DataBag innerBag = BAG_FACTORY.newDefaultBag();
      innerBag.add(TUPLE_FACTORY.newTuple(Float.valueOf(1)));
      innerBag.add(null);
      bag.add(TUPLE_FACTORY.newTuple(innerBag));
    }

    { // this is to simulate an output from a prior call of Intermediate
      final KllFloatsSketch qs = new KllFloatsSketch();
      qs.update(2);
      bag.add(TUPLE_FACTORY.newTuple(new DataByteArray(qs.toByteArray())));
    }

    final DataByteArray result = func.exec(TUPLE_FACTORY.newTuple(bag));
    final KllFloatsSketch sketch = getSketch(result);
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertEquals(sketch.getN(), 2);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void algebraicFinalWrongType() throws Exception {
    @SuppressWarnings("unchecked")
    final EvalFunc<DataByteArray> func = (EvalFunc<DataByteArray>) Class.forName(new DataToSketch().getFinal()).newInstance();
    final DataBag bag = BAG_FACTORY.newDefaultBag();

    // this bag must have tuples with either bags or data byte arrays
    bag.add(TUPLE_FACTORY.newTuple(1.0));
    func.exec(TUPLE_FACTORY.newTuple(bag));
  }

  // end of tests

  private static KllFloatsSketch getSketch(final Tuple tuple) throws Exception {
    Assert.assertNotNull(tuple);
    Assert.assertEquals(tuple.size(), 1);
    final DataByteArray bytes = (DataByteArray) tuple.get(0);
    return getSketch(bytes);
  }

  private static KllFloatsSketch getSketch(final DataByteArray bytes) throws Exception {
    Assert.assertTrue(bytes.size() > 0);
    return KllFloatsSketch.heapify(Memory.wrap(bytes.get()));
  }

}
