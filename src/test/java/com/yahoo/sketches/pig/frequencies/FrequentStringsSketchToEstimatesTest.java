package com.yahoo.sketches.pig.frequencies;

import java.util.Iterator;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.sketches.frequencies.ArrayOfStringsSerDe;
import com.yahoo.sketches.frequencies.FrequentItemsSketch;
import com.yahoo.sketches.pig.tuple.PigUtil;

public class FrequentStringsSketchToEstimatesTest {
  @Test
  public void nullInput() throws Exception {
    EvalFunc<DataBag> func = new FrequentStringsSketchToEstimates();
    DataBag bag = func.exec(null);
    Assert.assertNull(bag);
  }

  @Test
  public void emptyInput() throws Exception {
    EvalFunc<DataBag> func = new FrequentStringsSketchToEstimates();
    DataBag bag = func.exec(TupleFactory.getInstance().newTuple());
    Assert.assertNull(bag);
  }

  @Test
  public void emptySketch() throws Exception {
    EvalFunc<DataBag> func = new FrequentStringsSketchToEstimates();
    FrequentItemsSketch<String> sketch = new FrequentItemsSketch<String>(8);
    Tuple inputTuple = PigUtil.objectsToTuple(new DataByteArray(sketch.serializeToByteArray(new ArrayOfStringsSerDe())));
    DataBag bag = func.exec(inputTuple);
    Assert.assertNotNull(bag);
    Assert.assertEquals(bag.size(), 0);
  }

  @Test
  public void normalCase() throws Exception {
    EvalFunc<DataBag> func = new FrequentStringsSketchToEstimates();
    FrequentItemsSketch<String> sketch = new FrequentItemsSketch<String>(8);
    sketch.update("a");
    sketch.update("a");
    sketch.update("b");
    Tuple inputTuple = PigUtil.objectsToTuple(new DataByteArray(sketch.serializeToByteArray(new ArrayOfStringsSerDe())));
    DataBag bag = func.exec(inputTuple);
    Assert.assertNotNull(bag);
    Assert.assertEquals(bag.size(), 2);

    Iterator<Tuple> it = bag.iterator();
    Tuple tuple1 = it.next();
    Assert.assertEquals(tuple1.size(), 4);
    Assert.assertEquals((String)tuple1.get(0), "a");
    Assert.assertEquals((long)tuple1.get(1), 2L);
    Assert.assertEquals((long)tuple1.get(2), 2L);
    Assert.assertEquals((long)tuple1.get(3), 2L);

    Tuple tuple2 = it.next();
    Assert.assertEquals(tuple2.size(), 4);
    Assert.assertEquals((String)tuple2.get(0), "b");
    Assert.assertEquals((long)tuple2.get(1), 1L);
    Assert.assertEquals((long)tuple2.get(2), 1L);
    Assert.assertEquals((long)tuple2.get(3), 1L);
  }

  @Test
  public void schema() throws Exception {
    EvalFunc<DataBag> func = new FrequentStringsSketchToEstimates();
    Schema schema = func.outputSchema(null);
    Assert.assertNotNull(schema);
    Assert.assertEquals(schema.size(), 1);
    Assert.assertEquals(schema.getField(0).type, DataType.BAG);
    Assert.assertEquals(schema.getField(0).schema.size(), 1);
    Assert.assertEquals(schema.getField(0).schema.getField(0).type, DataType.TUPLE);
    Assert.assertEquals(schema.getField(0).schema.getField(0).schema.size(), 4);
    Assert.assertEquals(schema.getField(0).schema.getField(0).schema.getField(0).type, DataType.CHARARRAY);
    Assert.assertEquals(schema.getField(0).schema.getField(0).schema.getField(1).type, DataType.LONG);
    Assert.assertEquals(schema.getField(0).schema.getField(0).schema.getField(2).type, DataType.LONG);
    Assert.assertEquals(schema.getField(0).schema.getField(0).schema.getField(3).type, DataType.LONG);
  }
}
