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

package org.apache.datasketches.pig.sampling;

import static org.apache.datasketches.pig.sampling.ReservoirSampling.K_ALIAS;
import static org.apache.datasketches.pig.sampling.ReservoirSampling.N_ALIAS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.IOException;
import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.testng.annotations.Test;

@SuppressWarnings("javadoc")
public class ReservoirUnionTest {

  @SuppressWarnings("unused")
  @Test
  public void invalidMaxKTest() {
    try {
      new ReservoirUnion("1");
      fail();
    } catch (final IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  public void accumulateTest() {
    try {
      final long n = 20;
      final int k = 64;

      final Tuple reservoir1 = TupleFactory.getInstance().newTuple(3);
      reservoir1.set(0, n);
      reservoir1.set(1, k);
      reservoir1.set(2, ReservoirSamplingTest.generateDataBag(n, 0));

      final Tuple reservoir2 = TupleFactory.getInstance().newTuple(3);
      reservoir2.set(0, n);
      reservoir2.set(1, k);
      reservoir2.set(2, ReservoirSamplingTest.generateDataBag(n, (int) n));

      final Tuple reservoir3 = TupleFactory.getInstance().newTuple(3);
      reservoir3.set(0, n);
      reservoir3.set(1, k);
      reservoir3.set(2, ReservoirSamplingTest.generateDataBag(n, (int) (2 * n)));

      final DataBag bag1 = BagFactory.getInstance().newDefaultBag();
      bag1.add(reservoir1);
      bag1.add(reservoir2);
      final Tuple input1 = TupleFactory.getInstance().newTuple(bag1);

      final DataBag bag2 = BagFactory.getInstance().newDefaultBag();
      bag2.add(reservoir3);
      final Tuple input2 = TupleFactory.getInstance().newTuple(bag2);

      final ReservoirUnion ru = new ReservoirUnion(Integer.toString(k));
      ru.accumulate(input1);
      ru.accumulate(input2);

      final Tuple result = ru.getValue();

      // assuming k >= 3n so all items still in reservoir, in order
      assertEquals(result.size(), 3, "Unexpected tuple size from UDF");
      assertEquals((long) result.get(0), 3 * n, "Incorrect total number of items seen");
      assertEquals((int) result.get(1), k, "Unexpected value of k");

      final DataBag outputSamples = (DataBag) result.get(2);
      assertEquals(outputSamples.size(), (long) result.get(0),
              "Output reservoir size does not match reported number of items");
      int i = 0;
      for (Tuple t : outputSamples) {
        // expected format: (i:int, -i:chararray)
        assertEquals((int) t.get(0), i);
        assertEquals((String) t.get(1), Integer.toString(-i));
        ++i;
      }

      ru.cleanup();
      assertNull(ru.getValue());
    } catch (final IOException e) {
      fail("Unexpected exception");
    }
  }

  @Test
  public void checkMaxKLimit() {
    try {
      final int k = 64;
      final int maxK = 32;

      final Tuple reservoir = TupleFactory.getInstance().newTuple(3);
      reservoir.set(0, (long) k);
      reservoir.set(1, k);
      reservoir.set(2, ReservoirSamplingTest.generateDataBag(k, 0));

      final DataBag inputBag = BagFactory.getInstance().newDefaultBag();
      inputBag.add(reservoir);
      final Tuple inputTuple = TupleFactory.getInstance().newTuple(inputBag);

      final ReservoirUnion ru = new ReservoirUnion(Integer.toString(maxK));
      final Tuple result = ru.exec(inputTuple);

      assertEquals(result.size(), 3, "Unexpected tuple size from UDF");
      assertEquals((long) result.get(0), k, "Incorrect total number of items seen");
      assertEquals((int) result.get(1), maxK, "Unexpected value of k");

      final DataBag outputSamples = (DataBag) result.get(2);
      assertEquals(outputSamples.size(), maxK,
              "Output reservoir size does not match maxK");
      for (Tuple t : outputSamples) {
        // expected format: (i:int, -i:chararray)
        final int i = (int) t.get(0);
        assertTrue((i >= 0) && (i < k));
        assertEquals((String) t.get(1), Integer.toString(-i));
      }
    } catch (final IOException e) {
      fail("Unexpected exception");
    }
  }

  @Test
  public void checkDegenerateInput() {
    // using default max k value
    final ReservoirUnion ru = new ReservoirUnion();
    Tuple inputTuple;

    try {
      // input == null
      assertNull(ru.exec(null));

      // input.size() < 1
      inputTuple = TupleFactory.getInstance().newTuple(0);
      assertNull(ru.exec(inputTuple));

      // input.isNull(0);
      inputTuple = TupleFactory.getInstance().newTuple(1);
      inputTuple.set(0, null);
      assertNull(ru.exec(inputTuple));
    } catch (final IOException e) {
      fail("Unexpected exception");
    }

    try {
      // reservoir tuple with only 2 entries
      final Tuple reservoir = TupleFactory.getInstance().newTuple(2);
      reservoir.set(0, 256L);
      reservoir.set(1, 256);

      final DataBag reservoirBag = BagFactory.getInstance().newDefaultBag();
      reservoirBag.add(reservoir);

      inputTuple = TupleFactory.getInstance().newTuple(reservoirBag);
      ru.exec(inputTuple);
      fail("Did not catch expected ExecException");
    } catch (final ExecException e) {
      // expected
    } catch (final IOException e) {
      fail("Unexpected exception");
    }
  }

  @Test
  public void outputSchemaTest() throws FrontendException {
    final ReservoirUnion ru = new ReservoirUnion("5");

    final Schema tupleSchema = new Schema();
    tupleSchema.add(new Schema.FieldSchema("field1", DataType.CHARARRAY));
    tupleSchema.add(new Schema.FieldSchema("field2", DataType.INTEGER));
    final Schema recordSchema = new Schema();
    recordSchema.add(new Schema.FieldSchema("record", tupleSchema, DataType.TUPLE));

    final Schema sketchSchema = new Schema();
    sketchSchema.add(new Schema.FieldSchema(N_ALIAS, DataType.LONG));
    sketchSchema.add(new Schema.FieldSchema(K_ALIAS, DataType.INTEGER));
    // intentionally not using named constant to test pass-through
    sketchSchema.add(new Schema.FieldSchema("reservoir", recordSchema, DataType.BAG));
    final Schema sketchBagSchema = new Schema(new Schema.FieldSchema("sketch", sketchSchema, DataType.TUPLE));
    final Schema inputSchema = new Schema(new Schema.FieldSchema("sketchSet", sketchBagSchema, DataType.BAG));

    final Schema output = ru.outputSchema(inputSchema);
    assertEquals(output.size(), 1);

    final List<Schema.FieldSchema> outputFields = output.getField(0).schema.getFields();
    assertEquals(outputFields.size(), 3);

    // check high-level structure
    assertEquals(outputFields.get(0).alias, N_ALIAS);
    assertEquals(outputFields.get(0).type, DataType.LONG);
    assertEquals(outputFields.get(1).alias, K_ALIAS);
    assertEquals(outputFields.get(1).type, DataType.INTEGER);
    assertEquals(outputFields.get(2).alias, "reservoir"); // should pass through
    assertEquals(outputFields.get(2).type, DataType.BAG);

    // validate sample bag
    final Schema sampleSchema = outputFields.get(2).schema;
    assertTrue(recordSchema.equals(sampleSchema));
  }

  @Test
  public void degenerateSchemaTest() throws FrontendException {
    final ReservoirUnion ru = new ReservoirUnion("5");
    Schema output = ru.outputSchema(null);
    assertNull(output);

    output = ru.outputSchema(new Schema());
    assertNull(output);

    final Schema tupleSchema = new Schema();
    tupleSchema.add(new Schema.FieldSchema("field1", DataType.CHARARRAY));
    tupleSchema.add(new Schema.FieldSchema("field2", DataType.INTEGER));
    final Schema recordSchema = new Schema();
    recordSchema.add(new Schema.FieldSchema("record", tupleSchema, DataType.TUPLE));
    output = ru.outputSchema(new Schema());
    assertNull(output);
  }


}
