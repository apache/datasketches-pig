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

public class UnionSketchTest {

  private static final TupleFactory tupleFactory = TupleFactory.getInstance();
  private static final BagFactory bagFactory = BagFactory.getInstance();

  @Test
  public void execNullInputTuple() throws Exception {
    EvalFunc<DataByteArray> func = new UnionSketch();
    DataByteArray result = func.exec(null);
    HllSketch sketch = DataToSketchTest.getSketch(result);
    Assert.assertTrue(sketch.isEmpty());
  }

  @Test
  public void execEmptyInputTuple() throws Exception {
    EvalFunc<DataByteArray> func = new UnionSketch("10");
    DataByteArray result = func.exec(tupleFactory.newTuple());
    HllSketch sketch = DataToSketchTest.getSketch(result);
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertEquals(sketch.getLgConfigK(), 10);
  }

  @Test
  public void execEmptyBag() throws Exception {
    EvalFunc<DataByteArray> func = new UnionSketch("10", "HLL_6");
    DataByteArray result = func.exec(tupleFactory.newTuple(bagFactory.newDefaultBag()));
    HllSketch sketch = DataToSketchTest.getSketch(result);
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertEquals(sketch.getLgConfigK(), 10);
    Assert.assertEquals(sketch.getTgtHllType(), TgtHllType.HLL_6);
  }

  @Test
  public void execNormalCase() throws Exception {
    EvalFunc<DataByteArray> func = new UnionSketch();
    HllSketch inputSketch = new HllSketch(12);
    inputSketch.update(1);
    inputSketch.update(2);
    DataBag bag = bagFactory.newDefaultBag();
    bag.add(tupleFactory.newTuple(new DataByteArray(inputSketch.toCompactByteArray())));
    DataByteArray result = func.exec(tupleFactory.newTuple(bag));
    HllSketch sketch = DataToSketchTest.getSketch(result);
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertEquals(sketch.getEstimate(), 2.0, 0.01);
  }

  @Test
  public void accumulator() throws Exception {
    Accumulator<DataByteArray> func = new UnionSketch();

    // no input yet
    DataByteArray result = func.getValue();
    HllSketch sketch = DataToSketchTest.getSketch(result);
    Assert.assertTrue(sketch.isEmpty());

    // null input tuple
    func.accumulate(null);
    result = func.getValue();
    sketch = DataToSketchTest.getSketch(result);
    Assert.assertTrue(sketch.isEmpty());

    // empty input tuple
    func.accumulate(tupleFactory.newTuple());
    result = func.getValue();
    sketch = DataToSketchTest.getSketch(result);
    Assert.assertTrue(sketch.isEmpty());

    // empty bag
    func.accumulate(tupleFactory.newTuple(bagFactory.newDefaultBag()));
    result = func.getValue();
    sketch = DataToSketchTest.getSketch(result);
    Assert.assertTrue(sketch.isEmpty());

    // normal case
    HllSketch inputSketch = new HllSketch(12);
    inputSketch.update(1);
    inputSketch.update(2);
    DataBag bag = bagFactory.newDefaultBag();
    bag.add(tupleFactory.newTuple(new DataByteArray(inputSketch.toCompactByteArray())));
    func.accumulate(tupleFactory.newTuple(bag));
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
    EvalFunc<Tuple> func =
        (EvalFunc<Tuple>) Class.forName(new UnionSketch().getInitial()).newInstance();
    Tuple input = tupleFactory.newTuple();
    Tuple result = func.exec(input);
    Assert.assertEquals(result, input);
  }

  @Test
  public void algebraicIntermediateNullInputTuple() throws Exception {
    @SuppressWarnings("unchecked")
    EvalFunc<Tuple> func =
        (EvalFunc<Tuple>) Class.forName(new UnionSketch().getIntermed()).newInstance();
    Tuple result = func.exec(null);
    HllSketch sketch = DataToSketchTest.getSketch((DataByteArray) result.get(0));
    Assert.assertTrue(sketch.isEmpty());
  }

  @Test
  public void algebraicIntermediateEmptyInputTuple() throws Exception {
    @SuppressWarnings("unchecked")
    EvalFunc<Tuple> func =
        (EvalFunc<Tuple>) Class.forName(new UnionSketch().getIntermed())
        .getConstructor(String.class).newInstance("10");
    Tuple result = func.exec(tupleFactory.newTuple());
    HllSketch sketch = DataToSketchTest.getSketch((DataByteArray) result.get(0));
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertEquals(sketch.getLgConfigK(), 10);
  }

  @Test
  public void algebraicIntermediateFromInitial() throws Exception {
    @SuppressWarnings("unchecked")
    EvalFunc<Tuple> func =
        (EvalFunc<Tuple>) Class.forName(new UnionSketch().getIntermed())
        .getConstructor(String.class, String.class).newInstance("10", "HLL_6");
    HllSketch inputSketch = new HllSketch(12);
    inputSketch.update(1);
    inputSketch.update(2);
    inputSketch.update(3);
    DataBag outerBag = bagFactory.newDefaultBag();
    DataBag innerBag = bagFactory.newDefaultBag();
    innerBag.add(tupleFactory.newTuple(new DataByteArray(inputSketch.toCompactByteArray())));
    outerBag.add(tupleFactory.newTuple(innerBag));
    Tuple result = func.exec(tupleFactory.newTuple(outerBag));
    HllSketch sketch = DataToSketchTest.getSketch((DataByteArray) result.get(0));
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertEquals(sketch.getEstimate(), 3.0, 0.01);
    Assert.assertEquals(sketch.getLgConfigK(), 10);
    Assert.assertEquals(sketch.getTgtHllType(), TgtHllType.HLL_6);
  }

  @Test
  public void algebraicIntermediateFromIntermediate() throws Exception {
    @SuppressWarnings("unchecked")
    EvalFunc<Tuple> func =
        (EvalFunc<Tuple>) Class.forName(new UnionSketch().getIntermed()).newInstance();
    HllSketch inputSketch = new HllSketch(12);
    inputSketch.update("a");
    inputSketch.update("b");
    DataBag bag = bagFactory.newDefaultBag();
    bag.add(tupleFactory.newTuple(new DataByteArray(inputSketch.toCompactByteArray())));
    Tuple result = func.exec(tupleFactory.newTuple(bag));
    HllSketch sketch = DataToSketchTest.getSketch((DataByteArray) result.get(0));
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertEquals(sketch.getEstimate(), 2.0, 0.01);
  }

  @Test
  public void algebraicFinalNullInputTuple() throws Exception {
    @SuppressWarnings("unchecked")
    EvalFunc<DataByteArray> func =
        (EvalFunc<DataByteArray>) Class.forName(new UnionSketch().getFinal()).newInstance();
    DataByteArray result = func.exec(null);
    HllSketch sketch = DataToSketchTest.getSketch(result);
    Assert.assertTrue(sketch.isEmpty());
  }

  @Test
  public void algebraicFinalEmptyInputTuple() throws Exception {
    @SuppressWarnings("unchecked")
    EvalFunc<DataByteArray> func =
        (EvalFunc<DataByteArray>) Class.forName(new UnionSketch().getFinal())
        .getConstructor(String.class).newInstance("10");
    DataByteArray result = func.exec(tupleFactory.newTuple());
    HllSketch sketch = DataToSketchTest.getSketch(result);
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertEquals(sketch.getLgConfigK(), 10);
  }

  @Test
  public void algebraicFinalFromInitial() throws Exception {
    @SuppressWarnings("unchecked")
    EvalFunc<DataByteArray> func =
        (EvalFunc<DataByteArray>) Class.forName(new UnionSketch().getFinal())
        .getConstructor(String.class, String.class).newInstance("10", "HLL_6");
    HllSketch inputSketch = new HllSketch(12);
    inputSketch.update(1);
    inputSketch.update(2);
    inputSketch.update(3);
    DataBag outerBag = bagFactory.newDefaultBag();
    DataBag innerBag = bagFactory.newDefaultBag();
    innerBag.add(tupleFactory.newTuple(new DataByteArray(inputSketch.toCompactByteArray())));
    outerBag.add(tupleFactory.newTuple(innerBag));
    DataByteArray result = func.exec(tupleFactory.newTuple(outerBag));
    HllSketch sketch = DataToSketchTest.getSketch(result);
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertEquals(sketch.getEstimate(), 3.0, 0.01);
    Assert.assertEquals(sketch.getLgConfigK(), 10);
    Assert.assertEquals(sketch.getTgtHllType(), TgtHllType.HLL_6);
  }

  @Test
  public void algebraicFinalFromIntermediate() throws Exception {
    @SuppressWarnings("unchecked")
    EvalFunc<DataByteArray> func =
        (EvalFunc<DataByteArray>) Class.forName(new UnionSketch().getFinal()).newInstance();
    HllSketch inputSketch = new HllSketch(12);
    inputSketch.update("a");
    inputSketch.update("b");
    DataBag bag = bagFactory.newDefaultBag();
    bag.add(tupleFactory.newTuple(new DataByteArray(inputSketch.toCompactByteArray())));
    DataByteArray result = func.exec(tupleFactory.newTuple(bag));
    HllSketch sketch = DataToSketchTest.getSketch(result);
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertEquals(sketch.getEstimate(), 2.0, 0.01);
  }

}
