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

package org.apache.datasketches.pig.cpc;

import org.apache.pig.Accumulator;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.datasketches.cpc.CpcSketch;

@SuppressWarnings("javadoc")
public class UnionSketchTest {

  private static final TupleFactory TUPLE_FACTORY = TupleFactory.getInstance();
  private static final BagFactory BAG_FACTORY = BagFactory.getInstance();

  @Test
  public void execNullInputTuple() throws Exception {
    final EvalFunc<DataByteArray> func = new UnionSketch();
    final DataByteArray result = func.exec(null);
    final CpcSketch sketch = DataToSketchTest.getSketch(result);
    Assert.assertTrue(sketch.isEmpty());
  }

  @Test
  public void execEmptyInputTuple() throws Exception {
    final EvalFunc<DataByteArray> func = new UnionSketch();
    final DataByteArray result = func.exec(TUPLE_FACTORY.newTuple());
    final CpcSketch sketch = DataToSketchTest.getSketch(result);
    Assert.assertTrue(sketch.isEmpty());
  }

  @Test
  public void execEmptyBag() throws Exception {
    final EvalFunc<DataByteArray> func = new UnionSketch();
    final DataByteArray result = func.exec(TUPLE_FACTORY.newTuple(BAG_FACTORY.newDefaultBag()));
    final CpcSketch sketch = DataToSketchTest.getSketch(result);
    Assert.assertTrue(sketch.isEmpty());
  }

  @Test
  public void execNormalCase() throws Exception {
    final EvalFunc<DataByteArray> func = new UnionSketch();
    final CpcSketch inputSketch = new CpcSketch();
    inputSketch.update(1);
    inputSketch.update(2);
    final DataBag bag = BAG_FACTORY.newDefaultBag();
    bag.add(TUPLE_FACTORY.newTuple(new DataByteArray(inputSketch.toByteArray())));
    final DataByteArray result = func.exec(TUPLE_FACTORY.newTuple(bag));
    final CpcSketch sketch = DataToSketchTest.getSketch(result);
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertEquals(sketch.getEstimate(), 2.0, 0.01);
  }

  @Test
  public void execNormalCaseCustomLgKAndSeed() throws Exception {
    final EvalFunc<DataByteArray> func = new UnionSketch("10", "123");
    final CpcSketch inputSketch = new CpcSketch(10, 123);
    inputSketch.update(1);
    inputSketch.update(2);
    final DataBag bag = BAG_FACTORY.newDefaultBag();
    bag.add(TUPLE_FACTORY.newTuple(new DataByteArray(inputSketch.toByteArray())));
    final DataByteArray result = func.exec(TUPLE_FACTORY.newTuple(bag));
    final CpcSketch sketch = DataToSketchTest.getSketch(result, 123);
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertEquals(sketch.getEstimate(), 2.0, 0.01);
    Assert.assertEquals(sketch.getLgK(), 10);
  }

  @Test
  public void accumulator() throws Exception {
    final Accumulator<DataByteArray> func = new UnionSketch();

    // no input yet
    DataByteArray result = func.getValue();
    CpcSketch sketch = DataToSketchTest.getSketch(result);
    Assert.assertTrue(sketch.isEmpty());

    // null input tuple
    func.accumulate(null);
    result = func.getValue();
    sketch = DataToSketchTest.getSketch(result);
    Assert.assertTrue(sketch.isEmpty());

    // empty input tuple
    func.accumulate(TUPLE_FACTORY.newTuple());
    result = func.getValue();
    sketch = DataToSketchTest.getSketch(result);
    Assert.assertTrue(sketch.isEmpty());

    // empty bag
    func.accumulate(TUPLE_FACTORY.newTuple(BAG_FACTORY.newDefaultBag()));
    result = func.getValue();
    sketch = DataToSketchTest.getSketch(result);
    Assert.assertTrue(sketch.isEmpty());

    // normal case
    CpcSketch inputSketch = new CpcSketch();
    inputSketch.update(1);
    inputSketch.update(2);
    final DataBag bag = BAG_FACTORY.newDefaultBag();
    bag.add(TUPLE_FACTORY.newTuple(new DataByteArray(inputSketch.toByteArray())));
    func.accumulate(TUPLE_FACTORY.newTuple(bag));
    result = func.getValue();
    sketch = DataToSketchTest.getSketch(result);
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertEquals(sketch.getEstimate(), 2.0, 0.01);

    // cleanup
    func.cleanup();
    result = func.getValue();
    sketch = DataToSketchTest.getSketch(result);
    Assert.assertTrue(sketch.isEmpty());
  }

  @Test
  public void algebraicInitial() throws Exception {
    @SuppressWarnings("unchecked")
    final EvalFunc<Tuple> func =
        (EvalFunc<Tuple>) Class.forName(new UnionSketch().getInitial()).newInstance();
    final Tuple input = TUPLE_FACTORY.newTuple();
    final Tuple result = func.exec(input);
    Assert.assertEquals(result, input);
  }

  @Test
  public void algebraicIntermediateNullInputTuple() throws Exception {
    @SuppressWarnings("unchecked")
    final EvalFunc<Tuple> func =
        (EvalFunc<Tuple>) Class.forName(new UnionSketch().getIntermed()).newInstance();
    final Tuple result = func.exec(null);
    final CpcSketch sketch = DataToSketchTest.getSketch((DataByteArray) result.get(0));
    Assert.assertTrue(sketch.isEmpty());
  }

  @Test
  public void algebraicIntermediateEmptyInputTuple() throws Exception {
    @SuppressWarnings("unchecked")
    final EvalFunc<Tuple> func =
        (EvalFunc<Tuple>) Class.forName(new UnionSketch().getIntermed()).newInstance();
    final Tuple result = func.exec(TUPLE_FACTORY.newTuple());
    final CpcSketch sketch = DataToSketchTest.getSketch((DataByteArray) result.get(0));
    Assert.assertTrue(sketch.isEmpty());
  }

  @Test
  public void algebraicIntermediateFromInitial() throws Exception {
    @SuppressWarnings("unchecked")
    final EvalFunc<Tuple> func =
        (EvalFunc<Tuple>) Class.forName(new UnionSketch().getIntermed()).newInstance();
    final CpcSketch inputSketch = new CpcSketch();
    inputSketch.update(1);
    inputSketch.update(2);
    inputSketch.update(3);
    final DataBag outerBag = BAG_FACTORY.newDefaultBag();
    final DataBag innerBag = BAG_FACTORY.newDefaultBag();
    innerBag.add(TUPLE_FACTORY.newTuple(new DataByteArray(inputSketch.toByteArray())));
    outerBag.add(TUPLE_FACTORY.newTuple(innerBag));
    final Tuple result = func.exec(TUPLE_FACTORY.newTuple(outerBag));
    final CpcSketch sketch = DataToSketchTest.getSketch((DataByteArray) result.get(0));
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertEquals(sketch.getEstimate(), 3.0, 0.01);
  }

  @Test
  public void algebraicIntermediateFromIntermediate() throws Exception {
    @SuppressWarnings("unchecked")
    final EvalFunc<Tuple> func =
        (EvalFunc<Tuple>) Class.forName(new UnionSketch().getIntermed()).newInstance();
    final CpcSketch inputSketch = new CpcSketch();
    inputSketch.update("a");
    inputSketch.update("b");
    final DataBag bag = BAG_FACTORY.newDefaultBag();
    bag.add(TUPLE_FACTORY.newTuple(new DataByteArray(inputSketch.toByteArray())));
    final Tuple result = func.exec(TUPLE_FACTORY.newTuple(bag));
    final CpcSketch sketch = DataToSketchTest.getSketch((DataByteArray) result.get(0));
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertEquals(sketch.getEstimate(), 2.0, 0.01);
  }

  @Test
  public void algebraicIntermediateFromIntermediateCustomLgKAndSeed() throws Exception {
    @SuppressWarnings("unchecked")
    final EvalFunc<Tuple> func =
        (EvalFunc<Tuple>) Class.forName(new UnionSketch().getIntermed())
        .getConstructor(String.class, String.class).newInstance("10", "123");
    final CpcSketch inputSketch = new CpcSketch(10, 123);
    inputSketch.update("a");
    inputSketch.update("b");
    final DataBag bag = BAG_FACTORY.newDefaultBag();
    bag.add(TUPLE_FACTORY.newTuple(new DataByteArray(inputSketch.toByteArray())));
    final Tuple result = func.exec(TUPLE_FACTORY.newTuple(bag));
    final CpcSketch sketch = DataToSketchTest.getSketch((DataByteArray) result.get(0), 123);
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertEquals(sketch.getEstimate(), 2.0, 0.01);
    Assert.assertEquals(sketch.getLgK(), 10);
  }

  @Test
  public void algebraicFinalNullInputTuple() throws Exception {
    @SuppressWarnings("unchecked")
    final EvalFunc<DataByteArray> func =
        (EvalFunc<DataByteArray>) Class.forName(new UnionSketch().getFinal()).newInstance();
    final DataByteArray result = func.exec(null);
    final CpcSketch sketch = DataToSketchTest.getSketch(result);
    Assert.assertTrue(sketch.isEmpty());
  }

  @Test
  public void algebraicFinalEmptyInputTuple() throws Exception {
    @SuppressWarnings("unchecked")
    final EvalFunc<DataByteArray> func =
        (EvalFunc<DataByteArray>) Class.forName(new UnionSketch().getFinal()).newInstance();
    final DataByteArray result = func.exec(TUPLE_FACTORY.newTuple());
    final CpcSketch sketch = DataToSketchTest.getSketch(result);
    Assert.assertTrue(sketch.isEmpty());
  }

  @Test
  public void algebraicFinalFromInitial() throws Exception {
    @SuppressWarnings("unchecked")
    final EvalFunc<DataByteArray> func =
        (EvalFunc<DataByteArray>) Class.forName(new UnionSketch().getFinal()).newInstance();
    final CpcSketch inputSketch = new CpcSketch();
    inputSketch.update(1);
    inputSketch.update(2);
    inputSketch.update(3);
    final DataBag outerBag = BAG_FACTORY.newDefaultBag();
    final DataBag innerBag = BAG_FACTORY.newDefaultBag();
    innerBag.add(TUPLE_FACTORY.newTuple(new DataByteArray(inputSketch.toByteArray())));
    outerBag.add(TUPLE_FACTORY.newTuple(innerBag));
    final DataByteArray result = func.exec(TUPLE_FACTORY.newTuple(outerBag));
    final CpcSketch sketch = DataToSketchTest.getSketch(result);
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertEquals(sketch.getEstimate(), 3.0, 0.01);
  }

  @Test
  public void algebraicFinalFromInitialCustomLgKAndSeed() throws Exception {
    @SuppressWarnings("unchecked")
    final EvalFunc<DataByteArray> func =
        (EvalFunc<DataByteArray>) Class.forName(new UnionSketch().getFinal())
        .getConstructor(String.class, String.class).newInstance("10", "123");
    final CpcSketch inputSketch = new CpcSketch(10,123);
    inputSketch.update(1);
    inputSketch.update(2);
    inputSketch.update(3);
    final DataBag outerBag = BAG_FACTORY.newDefaultBag();
    final DataBag innerBag = BAG_FACTORY.newDefaultBag();
    innerBag.add(TUPLE_FACTORY.newTuple(new DataByteArray(inputSketch.toByteArray())));
    outerBag.add(TUPLE_FACTORY.newTuple(innerBag));
    final DataByteArray result = func.exec(TUPLE_FACTORY.newTuple(outerBag));
    final CpcSketch sketch = DataToSketchTest.getSketch(result, 123);
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertEquals(sketch.getEstimate(), 3.0, 0.01);
    Assert.assertEquals(sketch.getLgK(), 10);
  }

  @Test
  public void algebraicFinalFromIntermediate() throws Exception {
    @SuppressWarnings("unchecked")
    final EvalFunc<DataByteArray> func =
        (EvalFunc<DataByteArray>) Class.forName(new UnionSketch().getFinal()).newInstance();
    final CpcSketch inputSketch = new CpcSketch();
    inputSketch.update("a");
    inputSketch.update("b");
    final DataBag bag = BAG_FACTORY.newDefaultBag();
    bag.add(TUPLE_FACTORY.newTuple(new DataByteArray(inputSketch.toByteArray())));
    final DataByteArray result = func.exec(TUPLE_FACTORY.newTuple(bag));
    final CpcSketch sketch = DataToSketchTest.getSketch(result);
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertEquals(sketch.getEstimate(), 2.0, 0.01);
  }

}
