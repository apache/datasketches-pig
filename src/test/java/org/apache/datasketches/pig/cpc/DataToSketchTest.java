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

import static org.apache.datasketches.Util.DEFAULT_UPDATE_SEED;

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
public class DataToSketchTest {

  private static final TupleFactory TUPLE_FACTORY = TupleFactory.getInstance();
  private static final BagFactory BAG_FACTORY = BagFactory.getInstance();

  @Test
  public void execNullInputTuple() throws Exception {
    final EvalFunc<DataByteArray> func = new DataToSketch();
    final DataByteArray result = func.exec(null);
    final CpcSketch sketch = getSketch(result);
    Assert.assertTrue(sketch.isEmpty());
  }

  @Test
  public void execEmptyInputTupleCustomLgK() throws Exception {
    final EvalFunc<DataByteArray> func = new DataToSketch("10");
    final DataByteArray result = func.exec(TUPLE_FACTORY.newTuple());
    final CpcSketch sketch = getSketch(result);
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertEquals(sketch.getLgK(), 10);
  }

  @Test
  public void execEmptyBag() throws Exception {
    final EvalFunc<DataByteArray> func = new DataToSketch();
    final DataByteArray result = func.exec(TUPLE_FACTORY.newTuple(BAG_FACTORY.newDefaultBag()));
    final CpcSketch sketch = getSketch(result);
    Assert.assertTrue(sketch.isEmpty());
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void execUnsupportedType() throws Exception {
    final EvalFunc<DataByteArray> func = new DataToSketch();
    final DataBag bag = BAG_FACTORY.newDefaultBag();
    bag.add(TUPLE_FACTORY.newTuple(new Object()));
    func.exec(TUPLE_FACTORY.newTuple(bag));
  }

  @Test
  public void execVariousTypesOfInput() throws Exception {
    final EvalFunc<DataByteArray> func = new DataToSketch();
    final DataBag bag = BAG_FACTORY.newDefaultBag();
    final Tuple tupleWithNull = TUPLE_FACTORY.newTuple(1);
    tupleWithNull.set(0, null);
    bag.add(tupleWithNull);
    bag.add(TUPLE_FACTORY.newTuple(Byte.valueOf((byte) 1)));
    bag.add(TUPLE_FACTORY.newTuple(Integer.valueOf(2)));
    bag.add(TUPLE_FACTORY.newTuple(Long.valueOf(3L)));
    bag.add(TUPLE_FACTORY.newTuple(Float.valueOf(1.0f)));
    bag.add(TUPLE_FACTORY.newTuple(Double.valueOf(2.0)));
    bag.add(TUPLE_FACTORY.newTuple(new DataByteArray(new byte[] {(byte) 1})));
    bag.add(TUPLE_FACTORY.newTuple("a"));
    final CpcSketch sketch = getSketch(func.exec(TUPLE_FACTORY.newTuple(bag)));
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertEquals(sketch.getEstimate(), 7.0, 0.01);
  }

  @Test
  public void accumulator() throws Exception {
    final Accumulator<DataByteArray> func = new DataToSketch();

    // no input yet
    DataByteArray result = func.getValue();
    CpcSketch sketch = getSketch(result);
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
    final DataBag bag = BAG_FACTORY.newDefaultBag();
    bag.add(TUPLE_FACTORY.newTuple("a"));
    bag.add(TUPLE_FACTORY.newTuple("b"));
    func.accumulate(TUPLE_FACTORY.newTuple(bag));
    result = func.getValue();
    sketch = getSketch(result);
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertEquals(sketch.getEstimate(), 2.0, 0.01);

    // cleanup
    func.cleanup();
    result = func.getValue();
    sketch = getSketch(result);
    Assert.assertTrue(sketch.isEmpty());
  }

  @Test
  public void algebraicInitial() throws Exception {
    @SuppressWarnings("unchecked")
    final EvalFunc<Tuple> func =
        (EvalFunc<Tuple>) Class.forName(new DataToSketch().getInitial()).newInstance();
    final Tuple input = TUPLE_FACTORY.newTuple();
    final Tuple result = func.exec(input);
    Assert.assertEquals(result, input);
  }

  @Test
  public void algebraicInitialWithLgK() throws Exception {
    @SuppressWarnings("unchecked")
    final EvalFunc<Tuple> func =
        (EvalFunc<Tuple>) Class.forName(new DataToSketch().getInitial())
        .getConstructor(String.class).newInstance("10");
    final Tuple input = TUPLE_FACTORY.newTuple();
    final Tuple result = func.exec(input);
    Assert.assertEquals(result, input);
  }

  @Test
  public void algebraicInitialWithLgKAndSeed() throws Exception {
    @SuppressWarnings("unchecked")
    final EvalFunc<Tuple> func =
        (EvalFunc<Tuple>) Class.forName(new DataToSketch().getInitial())
        .getConstructor(String.class, String.class).newInstance("10", "123");
    final Tuple input = TUPLE_FACTORY.newTuple();
    final Tuple result = func.exec(input);
    Assert.assertEquals(result, input);
  }

  @Test
  public void algebraicIntermediateNullInputTuple() throws Exception {
    @SuppressWarnings("unchecked")
    final EvalFunc<Tuple> func =
        (EvalFunc<Tuple>) Class.forName(new DataToSketch().getIntermed()).newInstance();
    final Tuple result = func.exec(null);
    final CpcSketch sketch = getSketch((DataByteArray) result.get(0));
    Assert.assertTrue(sketch.isEmpty());
  }

  @Test
  public void algebraicIntermediateEmptyInputTuple() throws Exception {
    @SuppressWarnings("unchecked")
    final EvalFunc<Tuple> func =
        (EvalFunc<Tuple>) Class.forName(new DataToSketch().getIntermed()).newInstance();
    final Tuple result = func.exec(TUPLE_FACTORY.newTuple());
    final CpcSketch sketch = getSketch((DataByteArray) result.get(0));
    Assert.assertTrue(sketch.isEmpty());
  }

  @Test
  public void algebraicIntermediateEmptyBag() throws Exception {
    @SuppressWarnings("unchecked")
    final EvalFunc<Tuple> func =
        (EvalFunc<Tuple>) Class.forName(new DataToSketch().getIntermed()).newInstance();
    Tuple result = func.exec(TUPLE_FACTORY.newTuple(BAG_FACTORY.newDefaultBag()));
    CpcSketch sketch = getSketch((DataByteArray) result.get(0));
    Assert.assertTrue(sketch.isEmpty());
  }

  @Test
  public void algebraicIntermediateFromInitial() throws Exception {
    @SuppressWarnings("unchecked")
    final EvalFunc<Tuple> func =
        (EvalFunc<Tuple>) Class.forName(new DataToSketch().getIntermed()).newInstance();
    final DataBag outerBag = BAG_FACTORY.newDefaultBag();
    final DataBag innerBag = BAG_FACTORY.newDefaultBag();
    innerBag.add(TUPLE_FACTORY.newTuple("a"));
    innerBag.add(TUPLE_FACTORY.newTuple("b"));
    innerBag.add(TUPLE_FACTORY.newTuple("c"));
    outerBag.add(TUPLE_FACTORY.newTuple(innerBag));
    final Tuple result = func.exec(TUPLE_FACTORY.newTuple(outerBag));
    final CpcSketch sketch = getSketch((DataByteArray) result.get(0));
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertEquals(sketch.getEstimate(), 3.0, 0.01);
  }

  @Test
  public void algebraicIntermediateFromIntermediate() throws Exception {
    @SuppressWarnings("unchecked")
    final EvalFunc<Tuple> func =
        (EvalFunc<Tuple>) Class.forName(new DataToSketch().getIntermed()).newInstance();
    final CpcSketch inputSketch = new CpcSketch();
    inputSketch.update("a");
    inputSketch.update("b");
    final DataBag bag = BAG_FACTORY.newDefaultBag();
    bag.add(TUPLE_FACTORY.newTuple(new DataByteArray(inputSketch.toByteArray())));
    final Tuple result = func.exec(TUPLE_FACTORY.newTuple(bag));
    final CpcSketch sketch = getSketch((DataByteArray) result.get(0));
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertEquals(sketch.getEstimate(), 2.0, 0.01);
  }

  @Test
  public void algebraicIntermediateFromIntermediateCustomLgKAndSeed() throws Exception {
    @SuppressWarnings("unchecked")
    final EvalFunc<Tuple> func =
        (EvalFunc<Tuple>) Class.forName(new DataToSketch().getIntermed())
        .getConstructor(String.class, String.class).newInstance("10", "123");
    final CpcSketch inputSketch = new CpcSketch(10, 123);
    inputSketch.update("a");
    final DataBag bag = BAG_FACTORY.newDefaultBag();
    bag.add(TUPLE_FACTORY.newTuple(new DataByteArray(inputSketch.toByteArray())));
    final Tuple result = func.exec(TUPLE_FACTORY.newTuple(bag));
    final CpcSketch sketch = getSketch((DataByteArray) result.get(0), 123);
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertEquals(sketch.getEstimate(), 1.0, 0.01);
    Assert.assertEquals(sketch.getLgK(), 10);
  }

  @Test
  public void algebraicFinalNullInputTuple() throws Exception {
    @SuppressWarnings("unchecked")
    final EvalFunc<DataByteArray> func =
        (EvalFunc<DataByteArray>) Class.forName(new DataToSketch().getFinal()).newInstance();
    final DataByteArray result = func.exec(null);
    final CpcSketch sketch = getSketch(result);
    Assert.assertTrue(sketch.isEmpty());
  }

  @Test
  public void algebraicFinalEmptyInputTuple() throws Exception {
    @SuppressWarnings("unchecked")
    final EvalFunc<DataByteArray> func =
        (EvalFunc<DataByteArray>) Class.forName(new DataToSketch().getFinal()).newInstance();
    final DataByteArray result = func.exec(TUPLE_FACTORY.newTuple());
    final CpcSketch sketch = getSketch(result);
    Assert.assertTrue(sketch.isEmpty());
  }

  @Test
  public void algebraicFinalEmptyBag() throws Exception {
    @SuppressWarnings("unchecked")
    final EvalFunc<DataByteArray> func =
        (EvalFunc<DataByteArray>) Class.forName(new DataToSketch().getFinal()).newInstance();
    final DataByteArray result = func.exec(TUPLE_FACTORY.newTuple(BAG_FACTORY.newDefaultBag()));
    final CpcSketch sketch = getSketch(result);
    Assert.assertTrue(sketch.isEmpty());
  }

  @Test
  public void algebraicFinalFromInitial() throws Exception {
    @SuppressWarnings("unchecked")
    final EvalFunc<DataByteArray> func =
        (EvalFunc<DataByteArray>) Class.forName(new DataToSketch().getFinal()).newInstance();
    final DataBag outerBag = BAG_FACTORY.newDefaultBag();
    final DataBag innerBag = BAG_FACTORY.newDefaultBag();
    innerBag.add(TUPLE_FACTORY.newTuple("a"));
    innerBag.add(TUPLE_FACTORY.newTuple("b"));
    innerBag.add(TUPLE_FACTORY.newTuple("c"));
    outerBag.add(TUPLE_FACTORY.newTuple(innerBag));
    final DataByteArray result = func.exec(TUPLE_FACTORY.newTuple(outerBag));
    final CpcSketch sketch = getSketch(result);
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertEquals(sketch.getEstimate(), 3.0, 0.01);
  }

  @Test
  public void algebraicFinalFromIntermediate() throws Exception {
    @SuppressWarnings("unchecked")
    final EvalFunc<DataByteArray> func =
        (EvalFunc<DataByteArray>) Class.forName(new DataToSketch().getFinal()).newInstance();
    final CpcSketch inputSketch = new CpcSketch();
    inputSketch.update("a");
    inputSketch.update("b");
    DataBag bag = BAG_FACTORY.newDefaultBag();
    bag.add(TUPLE_FACTORY.newTuple(new DataByteArray(inputSketch.toByteArray())));
    DataByteArray result = func.exec(TUPLE_FACTORY.newTuple(bag));
    final CpcSketch sketch = getSketch(result);
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertEquals(sketch.getEstimate(), 2.0, 0.01);
  }

  @Test
  public void algebraicFinalFromIntermediateCustomLgKAndSeed() throws Exception {
    @SuppressWarnings("unchecked")
    final EvalFunc<DataByteArray> func =
        (EvalFunc<DataByteArray>) Class.forName(new DataToSketch().getFinal())
        .getConstructor(String.class, String.class).newInstance("10", "123");
    final CpcSketch inputSketch = new CpcSketch(10, 123);
    inputSketch.update("a");
    inputSketch.update("b");
    DataBag bag = BAG_FACTORY.newDefaultBag();
    bag.add(TUPLE_FACTORY.newTuple(new DataByteArray(inputSketch.toByteArray())));
    DataByteArray result = func.exec(TUPLE_FACTORY.newTuple(bag));
    final CpcSketch sketch = getSketch(result, 123);
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertEquals(sketch.getEstimate(), 2.0, 0.01);
    Assert.assertEquals(sketch.getLgK(), 10);
  }

  static CpcSketch getSketch(final DataByteArray dba) throws Exception {
    return getSketch(dba, DEFAULT_UPDATE_SEED);
  }

  static CpcSketch getSketch(final DataByteArray dba, final long seed) throws Exception {
    Assert.assertNotNull(dba);
    Assert.assertTrue(dba.size() > 0);
    return CpcSketch.heapify(dba.get(), seed);
  }

}
