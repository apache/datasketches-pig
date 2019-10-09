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

package org.apache.datasketches.pig.hll;

import org.apache.pig.Accumulator;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.TgtHllType;

@SuppressWarnings("javadoc")
public class DataToSketchTest {

  private static final TupleFactory tupleFactory = TupleFactory.getInstance();
  private static final BagFactory bagFactory = BagFactory.getInstance();

  @Test
  public void execNullInputTuple() throws Exception {
    EvalFunc<DataByteArray> func = new DataToSketch();
    DataByteArray result = func.exec(null);
    HllSketch sketch = getSketch(result);
    Assert.assertTrue(sketch.isEmpty());
  }

  @Test
  public void execEmptyInputTuple() throws Exception {
    EvalFunc<DataByteArray> func = new DataToSketch("10");
    DataByteArray result = func.exec(tupleFactory.newTuple());
    HllSketch sketch = getSketch(result);
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertEquals(sketch.getLgConfigK(), 10);
  }

  @Test
  public void execEmptyBag() throws Exception {
    EvalFunc<DataByteArray> func = new DataToSketch("10", "HLL_6");
    DataByteArray result = func.exec(tupleFactory.newTuple(bagFactory.newDefaultBag()));
    HllSketch sketch = getSketch(result);
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertEquals(sketch.getLgConfigK(), 10);
    Assert.assertEquals(sketch.getTgtHllType(), TgtHllType.HLL_6);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void execUnsupportedType() throws Exception {
    EvalFunc<DataByteArray> func = new DataToSketch();
    DataBag bag = bagFactory.newDefaultBag();
    bag.add(tupleFactory.newTuple(new Object()));
    func.exec(tupleFactory.newTuple(bag));
  }

  @Test
  public void execVariousTypesOfInput() throws Exception {
    EvalFunc<DataByteArray> func = new DataToSketch();
    DataBag bag = bagFactory.newDefaultBag();
    Tuple tupleWithNull = tupleFactory.newTuple(1);
    tupleWithNull.set(0, null);
    bag.add(tupleWithNull);
    bag.add(tupleFactory.newTuple(Byte.valueOf((byte) 1)));
    bag.add(tupleFactory.newTuple(Integer.valueOf(2)));
    bag.add(tupleFactory.newTuple(Long.valueOf(3)));
    bag.add(tupleFactory.newTuple(Float.valueOf(1)));
    bag.add(tupleFactory.newTuple(Double.valueOf(2)));
    bag.add(tupleFactory.newTuple(new DataByteArray(new byte[] {(byte) 1})));
    bag.add(tupleFactory.newTuple("a"));
    DataByteArray result = func.exec(tupleFactory.newTuple(bag));
    HllSketch sketch = getSketch(result);
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertEquals(sketch.getEstimate(), 7.0, 0.01);
  }

  @Test
  public void accumulator() throws Exception {
    Accumulator<DataByteArray> func = new DataToSketch();

    // no input yet
    DataByteArray result = func.getValue();
    HllSketch sketch = getSketch(result);
    Assert.assertTrue(sketch.isEmpty());

    // null input tuple
    func.accumulate(null);
    result = func.getValue();
    sketch = getSketch(result);
    Assert.assertTrue(sketch.isEmpty());

    // empty input tuple
    func.accumulate(tupleFactory.newTuple());
    result = func.getValue();
    sketch = getSketch(result);
    Assert.assertTrue(sketch.isEmpty());

    // empty bag
    func.accumulate(tupleFactory.newTuple(bagFactory.newDefaultBag()));
    result = func.getValue();
    sketch = getSketch(result);
    Assert.assertTrue(sketch.isEmpty());

    // normal case
    DataBag bag = bagFactory.newDefaultBag();
    bag.add(tupleFactory.newTuple("a"));
    bag.add(tupleFactory.newTuple("b"));
    func.accumulate(tupleFactory.newTuple(bag));
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
    EvalFunc<Tuple> func =
        (EvalFunc<Tuple>) Class.forName(new DataToSketch().getInitial()).newInstance();
    Tuple input = tupleFactory.newTuple();
    Tuple result = func.exec(input);
    Assert.assertEquals(result, input);
  }

  @Test
  public void algebraicInitialWithLgK() throws Exception {
    @SuppressWarnings("unchecked")
    EvalFunc<Tuple> func =
        (EvalFunc<Tuple>) Class.forName(new DataToSketch().getInitial())
        .getConstructor(String.class).newInstance("10");
    Tuple input = tupleFactory.newTuple();
    Tuple result = func.exec(input);
    Assert.assertEquals(result, input);
  }

  @Test
  public void algebraicInitialWithLgKAndTgtHllType() throws Exception {
    @SuppressWarnings("unchecked")
    EvalFunc<Tuple> func =
        (EvalFunc<Tuple>) Class.forName(new DataToSketch().getInitial())
        .getConstructor(String.class, String.class).newInstance("10", "HLL_6");
    Tuple input = tupleFactory.newTuple();
    Tuple result = func.exec(input);
    Assert.assertEquals(result, input);
  }

  @Test
  public void algebraicIntermediateNullInputTuple() throws Exception {
    @SuppressWarnings("unchecked")
    EvalFunc<Tuple> func =
        (EvalFunc<Tuple>) Class.forName(new DataToSketch().getIntermed()).newInstance();
    Tuple result = func.exec(null);
    HllSketch sketch = getSketch((DataByteArray) result.get(0));
    Assert.assertTrue(sketch.isEmpty());
  }

  @Test
  public void algebraicIntermediateEmptyInputTuple() throws Exception {
    @SuppressWarnings("unchecked")
    EvalFunc<Tuple> func =
        (EvalFunc<Tuple>) Class.forName(new DataToSketch().getIntermed())
        .getConstructor(String.class).newInstance("10");
    Tuple result = func.exec(tupleFactory.newTuple());
    HllSketch sketch = getSketch((DataByteArray) result.get(0));
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertEquals(sketch.getLgConfigK(), 10);
  }

  @Test
  public void algebraicIntermediateEmptyBag() throws Exception {
    @SuppressWarnings("unchecked")
    EvalFunc<Tuple> func =
        (EvalFunc<Tuple>) Class.forName(new DataToSketch().getIntermed())
        .getConstructor(String.class, String.class).newInstance("10", "HLL_6");
    Tuple result = func.exec(tupleFactory.newTuple(bagFactory.newDefaultBag()));
    HllSketch sketch = getSketch((DataByteArray) result.get(0));
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertEquals(sketch.getLgConfigK(), 10);
    Assert.assertEquals(sketch.getTgtHllType(), TgtHllType.HLL_6);
  }

  @Test
  public void algebraicIntermediateFromInitial() throws Exception {
    @SuppressWarnings("unchecked")
    EvalFunc<Tuple> func =
        (EvalFunc<Tuple>) Class.forName(new DataToSketch().getIntermed()).newInstance();
    DataBag outerBag = bagFactory.newDefaultBag();
    DataBag innerBag = bagFactory.newDefaultBag();
    innerBag.add(tupleFactory.newTuple("a"));
    innerBag.add(tupleFactory.newTuple("b"));
    innerBag.add(tupleFactory.newTuple("c"));
    outerBag.add(tupleFactory.newTuple(innerBag));
    Tuple result = func.exec(tupleFactory.newTuple(outerBag));
    HllSketch sketch = getSketch((DataByteArray) result.get(0));
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertEquals(sketch.getEstimate(), 3.0, 0.01);
  }

  @Test
  public void algebraicIntermediateFromIntermediate() throws Exception {
    @SuppressWarnings("unchecked")
    EvalFunc<Tuple> func =
        (EvalFunc<Tuple>) Class.forName(new DataToSketch().getIntermed()).newInstance();
    HllSketch inputSketch = new HllSketch(12);
    inputSketch.update("a");
    inputSketch.update("b");
    DataBag bag = bagFactory.newDefaultBag();
    bag.add(tupleFactory.newTuple(new DataByteArray(inputSketch.toCompactByteArray())));
    Tuple result = func.exec(tupleFactory.newTuple(bag));
    HllSketch sketch = getSketch((DataByteArray) result.get(0));
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertEquals(sketch.getEstimate(), 2.0, 0.01);
  }

  @Test
  public void algebraicFinalNullInputTuple() throws Exception {
    @SuppressWarnings("unchecked")
    EvalFunc<DataByteArray> func =
        (EvalFunc<DataByteArray>) Class.forName(new DataToSketch().getFinal()).newInstance();
    DataByteArray result = func.exec(null);
    HllSketch sketch = getSketch(result);
    Assert.assertTrue(sketch.isEmpty());
  }

  @Test
  public void algebraicFinalEmptyInputTuple() throws Exception {
    @SuppressWarnings("unchecked")
    EvalFunc<DataByteArray> func =
        (EvalFunc<DataByteArray>) Class.forName(new DataToSketch().getFinal())
        .getConstructor(String.class).newInstance("10");
    DataByteArray result = func.exec(tupleFactory.newTuple());
    HllSketch sketch = getSketch(result);
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertEquals(sketch.getLgConfigK(), 10);
  }

  @Test
  public void algebraicFinalEmptyBag() throws Exception {
    @SuppressWarnings("unchecked")
    EvalFunc<DataByteArray> func =
        (EvalFunc<DataByteArray>) Class.forName(new DataToSketch().getFinal())
        .getConstructor(String.class, String.class).newInstance("10", "HLL_6");
    DataByteArray result = func.exec(tupleFactory.newTuple(bagFactory.newDefaultBag()));
    HllSketch sketch = getSketch(result);
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertEquals(sketch.getLgConfigK(), 10);
    Assert.assertEquals(sketch.getTgtHllType(), TgtHllType.HLL_6);
  }

  @Test
  public void algebraicFinalFromInitial() throws Exception {
    @SuppressWarnings("unchecked")
    EvalFunc<DataByteArray> func =
        (EvalFunc<DataByteArray>) Class.forName(new DataToSketch().getFinal()).newInstance();
    DataBag outerBag = bagFactory.newDefaultBag();
    DataBag innerBag = bagFactory.newDefaultBag();
    innerBag.add(tupleFactory.newTuple("a"));
    innerBag.add(tupleFactory.newTuple("b"));
    innerBag.add(tupleFactory.newTuple("c"));
    outerBag.add(tupleFactory.newTuple(innerBag));
    DataByteArray result = func.exec(tupleFactory.newTuple(outerBag));
    HllSketch sketch = getSketch(result);
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertEquals(sketch.getEstimate(), 3.0, 0.01);
  }

  @Test
  public void algebraicFinalFromIntermediate() throws Exception {
    @SuppressWarnings("unchecked")
    EvalFunc<DataByteArray> func =
        (EvalFunc<DataByteArray>) Class.forName(new DataToSketch().getFinal()).newInstance();
    HllSketch inputSketch = new HllSketch(12);
    inputSketch.update("a");
    inputSketch.update("b");
    DataBag bag = bagFactory.newDefaultBag();
    bag.add(tupleFactory.newTuple(new DataByteArray(inputSketch.toCompactByteArray())));
    DataByteArray result = func.exec(tupleFactory.newTuple(bag));
    HllSketch sketch = getSketch(result);
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertEquals(sketch.getEstimate(), 2.0, 0.01);
  }

  static HllSketch getSketch(DataByteArray dba) throws Exception {
    Assert.assertNotNull(dba);
    Assert.assertTrue(dba.size() > 0);
    return HllSketch.heapify(dba.get());
  }

}
