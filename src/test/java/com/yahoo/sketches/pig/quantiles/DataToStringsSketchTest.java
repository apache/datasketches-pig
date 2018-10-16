/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.pig.quantiles;

import java.util.Comparator;

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

import com.yahoo.memory.Memory;
import com.yahoo.sketches.ArrayOfItemsSerDe;
import com.yahoo.sketches.ArrayOfStringsSerDe;
import com.yahoo.sketches.quantiles.ItemsSketch;

public class DataToStringsSketchTest {

  private static final TupleFactory TUPLE_FACTORY = TupleFactory.getInstance();
  private static final BagFactory BAG_FACTORY = BagFactory.getInstance();

  private static final Comparator<String> COMPARATOR = Comparator.naturalOrder();
  private static final ArrayOfItemsSerDe<String> SER_DE = new ArrayOfStringsSerDe();

  @Test
  public void execNullInputTuple() throws Exception {
    EvalFunc<Tuple> func = new DataToStringsSketch();
    Tuple resultTuple = func.exec(null);
    ItemsSketch<String> sketch = getSketch(resultTuple);
    Assert.assertTrue(sketch.isEmpty());
  }

  @Test
  public void execEmptyInputTuple() throws Exception {
    EvalFunc<Tuple> func = new DataToStringsSketch();
    Tuple resultTuple = func.exec(TUPLE_FACTORY.newTuple());
    ItemsSketch<String> sketch = getSketch(resultTuple);
    Assert.assertTrue(sketch.isEmpty());
  }

  @Test
  public void execEmptyBag() throws Exception {
    EvalFunc<Tuple> func = new DataToStringsSketch();
    Tuple resultTuple = func.exec(TUPLE_FACTORY.newTuple(BAG_FACTORY.newDefaultBag()));
    ItemsSketch<String> sketch = getSketch(resultTuple);
    Assert.assertTrue(sketch.isEmpty());
  }

  @Test(expectedExceptions = ClassCastException.class)
  public void execWrongValueType() throws Exception {
    EvalFunc<Tuple> func = new DataToStringsSketch();
    DataBag bag = BAG_FACTORY.newDefaultBag();
    bag.add(TUPLE_FACTORY.newTuple(1.0));
    func.exec(TUPLE_FACTORY.newTuple(bag));
  }

  @Test
  public void execNormalCase() throws Exception {
    EvalFunc<Tuple> func = new DataToStringsSketch();
    DataBag bag = BAG_FACTORY.newDefaultBag();
    bag.add(TUPLE_FACTORY.newTuple("a"));
    Tuple resultTuple = func.exec(TUPLE_FACTORY.newTuple(bag));
    ItemsSketch<String> sketch = getSketch(resultTuple);
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertEquals(sketch.getN(), 1);
  }
  
  @Test
  public void execMixedNormalCase() throws Exception {
    EvalFunc<Tuple> func = new DataToStringsSketch();
    DataBag bag = BAG_FACTORY.newDefaultBag();
    bag.add(TUPLE_FACTORY.newTuple("a"));
    bag.add(null);
    Tuple resultTuple = func.exec(TUPLE_FACTORY.newTuple(bag));
    ItemsSketch<String> sketch = getSketch(resultTuple);
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertEquals(sketch.getN(), 1);
  }

  @Test
  public void accumulator() throws Exception {
    Accumulator<Tuple> func = new DataToStringsSketch();

    // no input yet
    Tuple resultTuple = func.getValue();
    ItemsSketch<String> sketch = getSketch(resultTuple);
    Assert.assertTrue(sketch.isEmpty());

    // null input tuple
    func.accumulate(null);
    resultTuple = func.getValue();
    sketch = getSketch(resultTuple);
    Assert.assertTrue(sketch.isEmpty());

    // empty input tuple
    func.accumulate(TUPLE_FACTORY.newTuple());
    resultTuple = func.getValue();
    sketch = getSketch(resultTuple);
    Assert.assertTrue(sketch.isEmpty());

    // empty bag
    func.accumulate(TUPLE_FACTORY.newTuple(BAG_FACTORY.newDefaultBag()));
    resultTuple = func.getValue();
    sketch = getSketch(resultTuple);
    Assert.assertTrue(sketch.isEmpty());

    // normal case
    DataBag bag = BAG_FACTORY.newDefaultBag();
    bag.add(TUPLE_FACTORY.newTuple("a"));
    func.accumulate(TUPLE_FACTORY.newTuple(bag));
    func.accumulate(TUPLE_FACTORY.newTuple(bag));
    resultTuple = func.getValue();
    sketch = getSketch(resultTuple);
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertEquals(sketch.getN(), 2);
    
    // mixed null case
    bag = BAG_FACTORY.newDefaultBag();
    bag.add(TUPLE_FACTORY.newTuple("a"));
    bag.add(null);
    func.accumulate(TUPLE_FACTORY.newTuple(bag));
    func.accumulate(TUPLE_FACTORY.newTuple(bag));
    resultTuple = func.getValue();
    sketch = getSketch(resultTuple);
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertEquals(sketch.getN(), 2);

    // cleanup
    func.cleanup();
    resultTuple = func.getValue();
    sketch = getSketch(resultTuple);
    Assert.assertTrue(sketch.isEmpty());
  }

  @Test
  public void algebraicInitial() throws Exception {
    @SuppressWarnings("unchecked")
    EvalFunc<Tuple> func = (EvalFunc<Tuple>) Class.forName(new DataToStringsSketch().getInitial()).newInstance();
    DataBag bag = BAG_FACTORY.newDefaultBag();
    bag.add(TUPLE_FACTORY.newTuple());
    Tuple resultTuple = func.exec(TUPLE_FACTORY.newTuple(bag));
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 1);
    Assert.assertTrue(resultTuple.get(0) instanceof DataBag);
    Assert.assertEquals(((DataBag) resultTuple.get(0)).size(), 1);
  }

  @Test
  public void algebraicIntermediateIsSameAsFinal() {
    Assert.assertEquals(new DataToStringsSketch().getIntermed(), new DataToStringsSketch().getFinal());
  }

  @Test
  public void algebraicIntermediateFinalNullInputTuple() throws Exception {
    @SuppressWarnings("unchecked")
    EvalFunc<Tuple> func = (EvalFunc<Tuple>) Class.forName(new DataToStringsSketch().getIntermed()).newInstance();
    Tuple resultTuple = func.exec(null);
    ItemsSketch<String> sketch = getSketch(resultTuple);
    Assert.assertTrue(sketch.isEmpty());
  }

  @Test
  public void algebraicIntermediateFinalEmptyInputTuple() throws Exception {
    @SuppressWarnings("unchecked")
    EvalFunc<Tuple> func = (EvalFunc<Tuple>) Class.forName(new DataToStringsSketch().getIntermed()).newInstance();
    Tuple resultTuple = func.exec(TUPLE_FACTORY.newTuple());
    ItemsSketch<String> sketch = getSketch(resultTuple);
    Assert.assertTrue(sketch.isEmpty());
  }

  @Test
  public void algebraicIntermediateFinalNormalCase() throws Exception {
    @SuppressWarnings("unchecked")
    EvalFunc<Tuple> func = (EvalFunc<Tuple>) Class.forName(new DataToStringsSketch().getIntermed()).newInstance();
    DataBag bag = BAG_FACTORY.newDefaultBag();

    { // this is to simulate an output from Initial
      DataBag innerBag = BAG_FACTORY.newDefaultBag();
      innerBag.add(TUPLE_FACTORY.newTuple("a"));
      bag.add(TUPLE_FACTORY.newTuple(innerBag));
    }

    { // this is to simulate an output from a prior call of IntermediateFinal
      ItemsSketch<String> qs = ItemsSketch.getInstance(COMPARATOR);
      qs.update("b");
      bag.add(TUPLE_FACTORY.newTuple(new DataByteArray(qs.toByteArray(SER_DE))));
    }

    Tuple resultTuple = func.exec(TUPLE_FACTORY.newTuple(bag));
    ItemsSketch<String> sketch = getSketch(resultTuple);
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertEquals(sketch.getN(), 2);
  }
  
  @Test
  public void algebraicIntermediateFinalMixedNullCase() throws Exception {
    @SuppressWarnings("unchecked")
    EvalFunc<Tuple> func = (EvalFunc<Tuple>) Class.forName(new DataToStringsSketch().getIntermed()).newInstance();
    DataBag bag = BAG_FACTORY.newDefaultBag();

    { // this is to simulate an output from Initial
      DataBag innerBag = BAG_FACTORY.newDefaultBag();
      innerBag.add(TUPLE_FACTORY.newTuple("a"));
      innerBag.add(null);
      bag.add(TUPLE_FACTORY.newTuple(innerBag));
    }

    { // this is to simulate an output from a prior call of IntermediateFinal
      ItemsSketch<String> qs = ItemsSketch.getInstance(COMPARATOR);
      qs.update("b");
      bag.add(TUPLE_FACTORY.newTuple(new DataByteArray(qs.toByteArray(SER_DE))));
    }

    Tuple resultTuple = func.exec(TUPLE_FACTORY.newTuple(bag));
    ItemsSketch<String> sketch = getSketch(resultTuple);
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertEquals(sketch.getN(), 2);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void algebraicIntermediateFinalWrongType() throws Exception {
    @SuppressWarnings("unchecked")
    EvalFunc<Tuple> func = (EvalFunc<Tuple>) Class.forName(new DataToStringsSketch().getIntermed()).newInstance();
    DataBag bag = BAG_FACTORY.newDefaultBag();

    // this bag must have tuples with either bags or data byte arrays
    bag.add(TUPLE_FACTORY.newTuple("a"));
    func.exec(TUPLE_FACTORY.newTuple(bag));
  }

  @Test
  public void schema() throws Exception {
    EvalFunc<Tuple> func = new DataToStringsSketch();
    Schema schema = func.outputSchema(new Schema());
    Assert.assertNotNull(schema);
    Assert.assertEquals(schema.size(), 1);
    Assert.assertEquals(schema.getField(0).type, DataType.TUPLE);
    Assert.assertEquals(schema.getField(0).schema.size(), 1);
    Assert.assertEquals(schema.getField(0).schema.getField(0).type, DataType.BYTEARRAY);
  }

  // end of tests

  private static ItemsSketch<String> getSketch(Tuple tuple) throws Exception {
    Assert.assertNotNull(tuple);
    Assert.assertEquals(tuple.size(), 1);
    DataByteArray bytes = (DataByteArray) tuple.get(0);
    Assert.assertTrue(bytes.size() > 0);
    return ItemsSketch.getInstance(Memory.wrap(bytes.get()), COMPARATOR, SER_DE);
  }
}
