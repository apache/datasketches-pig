package com.yahoo.sketches.pig.sampling;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.IOException;

import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.sampling.VarOptItemsSketch;

public class DataToVarOptSketchTest {
  @Test
  public void checkConstructors() {
    // these three should work
    DataToVarOptSketch udf = new DataToVarOptSketch();
    assertNotNull(udf);

    udf = new DataToVarOptSketch("255");
    assertNotNull(udf);

    udf = new DataToVarOptSketch("123", "0");
    assertNotNull(udf);

    try {
      new DataToVarOptSketch("-1");
    } catch (final IllegalArgumentException e) {
      // expected
    }

    try {
      new DataToVarOptSketch("-1", "3");
    } catch (final IllegalArgumentException e) {
      // expected
    }

    try {
      new DataToVarOptSketch("10", "-1");
    } catch (final IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  public void checkExecution() {
    final int k = 10;
    final DataToVarOptSketch udf = new DataToVarOptSketch(Integer.toString(k), "0");

    final DataBag inputBag = BagFactory.getInstance().newDefaultBag();
    final Tuple inputTuple = TupleFactory.getInstance().newTuple(1);

    // calling accumulate() twice, but keep in exact mode so reference sketch has same values
    try {
      final VarOptItemsSketch<Tuple> sketch = VarOptItemsSketch.newInstance(k);
      for (int i = 1; i < k / 2; ++i) {
        final Tuple t = TupleFactory.getInstance().newTuple(3);
        t.set(0, 1.0 * i);
        t.set(1, i);
        t.set(2, -i);
        inputBag.add(t);
        sketch.update(t, 1.0 * i);
        sketch.update(t, 1.0 * i); // since calling accumulate() twice later
      }
      inputTuple.set(0, inputBag);

      assertNull(udf.getValue());
      udf.accumulate(inputTuple);
      udf.accumulate(inputTuple);
      final DataByteArray outBytes = udf.getValue();
      udf.cleanup();
      assertNull(udf.getValue());

      final VarOptItemsSketch<Tuple> result = VarOptItemsSketch.heapify(Memory.wrap(outBytes.get()),
              new ArrayOfTuplesSerDe());

      assertNotNull(result);
      VarOptCommonAlgebraicTest.compareResults(result, sketch);
    } catch (final IOException e) {
      fail("Unexpected exception");
    }
  }

  @Test
  public void degenerateExecInput() {
    final DataToVarOptSketch udf = new DataToVarOptSketch();

    try {
      assertNull(udf.exec(null));
      assertNull(udf.exec(TupleFactory.getInstance().newTuple(0)));

      final Tuple in = TupleFactory.getInstance().newTuple(1);
      in.set(0, null);
      assertNull(udf.exec(in));
    } catch (final IOException e) {
      fail("Unexpected exception");
    }
  }

  @Test
  public void validOutputSchemaTest() throws IOException {
    DataToVarOptSketch udf = new DataToVarOptSketch("5", "1");

    final Schema recordSchema = new Schema();
    recordSchema.add(new Schema.FieldSchema("field1", DataType.CHARARRAY));
    recordSchema.add(new Schema.FieldSchema("field2", DataType.DOUBLE));
    recordSchema.add(new Schema.FieldSchema("field3", DataType.FLOAT));
    final Schema tupleSchema = new Schema();
    tupleSchema.add(new Schema.FieldSchema("record", recordSchema, DataType.TUPLE));

    final Schema inputSchema = new Schema();
    inputSchema.add(new Schema.FieldSchema("data", tupleSchema, DataType.BAG));

    Schema output = udf.outputSchema(inputSchema);
    assertEquals(output.size(), 1);
    assertEquals(output.getField(0).type, DataType.BYTEARRAY);

    // use the float as a weight instead
    udf = new DataToVarOptSketch("5", "2");
    output = udf.outputSchema(inputSchema);
    assertEquals(output.size(), 1);
    assertEquals(output.getField(0).type, DataType.BYTEARRAY);
  }

  @Test
  public void badOutputSchemaTest() throws IOException {
    final Schema recordSchema = new Schema();
    recordSchema.add(new Schema.FieldSchema("field1", DataType.CHARARRAY));
    recordSchema.add(new Schema.FieldSchema("field2", DataType.DOUBLE));
    recordSchema.add(new Schema.FieldSchema("field3", DataType.INTEGER));
    final Schema tupleSchema = new Schema();
    tupleSchema.add(new Schema.FieldSchema("record", recordSchema, DataType.TUPLE));

    final Schema inputSchema = new Schema();
    inputSchema.add(new Schema.FieldSchema("data", tupleSchema, DataType.BAG));

    final DataToVarOptSketch udf = new DataToVarOptSketch("5", "0");

    // degenerate input schemas
    try {
      udf.outputSchema(null);
    } catch (final IllegalArgumentException e) {
      // expected
    }

    try {
      udf.outputSchema(new Schema());
    } catch (final IllegalArgumentException e) {
      // expected
    }

    // expecting weight in element 0
    try {
      udf.outputSchema(inputSchema);
    } catch (final IllegalArgumentException e) {
      // expected
    }

    // passing in Tuple instead of DataBag
    try {
      udf.outputSchema(tupleSchema);
    } catch (final IllegalArgumentException e) {
      // expected
    }
  }

}
