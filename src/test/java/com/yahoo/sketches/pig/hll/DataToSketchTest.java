/*
 * Copyright 2017, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.pig.hll;

import org.apache.pig.Accumulator;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.sketches.hll.HllSketch;
import com.yahoo.sketches.hll.TgtHllType;

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
    bag.add(tupleFactory.newTuple(new Byte((byte) 1)));
    bag.add(tupleFactory.newTuple(new Integer(2)));
    bag.add(tupleFactory.newTuple(new Long(3)));
    bag.add(tupleFactory.newTuple(new Float(1)));
    bag.add(tupleFactory.newTuple(new Double(2)));
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
    EvalFunc<Tuple> func =
        (EvalFunc<Tuple>) Class.forName(new DataToSketch().getInitial()).newInstance();
    Tuple input = tupleFactory.newTuple();
    Tuple result = func.exec(input);
    Assert.assertEquals(result, input);
  }

  @Test
  public void algebraicInitialWithLgK() throws Exception {
    EvalFunc<Tuple> func =
        (EvalFunc<Tuple>) Class.forName(new DataToSketch().getInitial())
        .getConstructor(String.class).newInstance("10");
    Tuple input = tupleFactory.newTuple();
    Tuple result = func.exec(input);
    Assert.assertEquals(result, input);
  }

  @Test
  public void algebraicInitialWithLgKAndTgtHllType() throws Exception {
    EvalFunc<Tuple> func =
        (EvalFunc<Tuple>) Class.forName(new DataToSketch().getInitial())
        .getConstructor(String.class, String.class).newInstance("10", "HLL_6");
    Tuple input = tupleFactory.newTuple();
    Tuple result = func.exec(input);
    Assert.assertEquals(result, input);
  }

  @Test
  public void algebraicIntermediateNullInputTuple() throws Exception {
    EvalFunc<Tuple> func =
        (EvalFunc<Tuple>) Class.forName(new DataToSketch().getIntermed()).newInstance();
    Tuple result = func.exec(null);
    HllSketch sketch = getSketch((DataByteArray) result.get(0));
    Assert.assertTrue(sketch.isEmpty());
  }

  @Test
  public void algebraicIntermediateEmptyInputTuple() throws Exception {
    EvalFunc<Tuple> func =
        (EvalFunc<Tuple>) Class.forName(new DataToSketch().getIntermed())
        .getConstructor(String.class).newInstance("10");
    Tuple result = func.exec(tupleFactory.newTuple());
    HllSketch sketch = getSketch((DataByteArray) result.get(0));
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertEquals(sketch.getLgConfigK(), 10);
  }

  @Test
  public void algebraicIntermediateFromInitial() throws Exception {
    EvalFunc<Tuple> func =
        (EvalFunc<Tuple>) Class.forName(new DataToSketch().getIntermed())
        .getConstructor(String.class, String.class).newInstance("10", "HLL_6");
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
    Assert.assertEquals(sketch.getLgConfigK(), 10);
    Assert.assertEquals(sketch.getTgtHllType(), TgtHllType.HLL_6);
  }

  @Test
  public void algebraicIntermediateFromIntermediate() throws Exception {
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
    EvalFunc<DataByteArray> func =
        (EvalFunc<DataByteArray>) Class.forName(new DataToSketch().getFinal()).newInstance();
    DataByteArray result = func.exec(null);
    HllSketch sketch = getSketch(result);
    Assert.assertTrue(sketch.isEmpty());
  }

  @Test
  public void algebraicFinalEmptyInputTuple() throws Exception {
    EvalFunc<DataByteArray> func =
        (EvalFunc<DataByteArray>) Class.forName(new DataToSketch().getFinal())
        .getConstructor(String.class).newInstance("10");
    DataByteArray result = func.exec(tupleFactory.newTuple());
    HllSketch sketch = getSketch(result);
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertEquals(sketch.getLgConfigK(), 10);
  }

  @Test
  public void algebraicFinalFromInitial() throws Exception {
    EvalFunc<DataByteArray> func =
        (EvalFunc<DataByteArray>) Class.forName(new DataToSketch().getFinal())
        .getConstructor(String.class, String.class).newInstance("10", "HLL_6");
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
    Assert.assertEquals(sketch.getLgConfigK(), 10);
    Assert.assertEquals(sketch.getTgtHllType(), TgtHllType.HLL_6);
  }

  @Test
  public void algebraicFinalFromIntermediate() throws Exception {
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
