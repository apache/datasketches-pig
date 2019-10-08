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

import static org.apache.datasketches.pig.sampling.VarOptCommonImpl.RECORD_ALIAS;
import static org.apache.datasketches.pig.sampling.VarOptCommonImpl.WEIGHT_ALIAS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.IOException;
import java.util.List;

import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.testng.annotations.Test;

import org.apache.datasketches.sampling.VarOptItemsSketch;

@SuppressWarnings("javadoc")
public class VarOptSamplingTest {
  static final double EPS = 1e-10;
  private static final ArrayOfTuplesSerDe serDe_ = new ArrayOfTuplesSerDe();

  @SuppressWarnings("unused")
  @Test
  public void baseConstructors() {
    // these three should work
    VarOptSampling udf = new VarOptSampling();
    assertNotNull(udf);

    udf = new VarOptSampling("255");
    assertNotNull(udf);

    udf = new VarOptSampling("123", "0");
    assertNotNull(udf);

    try {
      new VarOptSampling("-1");
      fail("Accepted negative k");
    } catch (final IllegalArgumentException e) {
      // expected
    }

    try {
      new VarOptSampling("-1", "3");
      fail("Accepted negative k");
    } catch (final IllegalArgumentException e) {
      // expected
    }

    try {
      new VarOptSampling("10", "-1");
      fail("Accepted negative weight index");
    } catch (final IllegalArgumentException e) {
      // expected
    }
  }

  @SuppressWarnings("unused")
  @Test
  public void algebraicFinalConstructors() {
    // these there should work
    VarOptSampling.Final udf = new VarOptSampling.Final();
    assertNotNull(udf);

    udf = new VarOptSampling.Final("1024");
    assertNotNull(udf);

    udf = new VarOptSampling.Final("4239", "2");
    assertNotNull(udf);

    try {
      new VarOptSampling.Final("-1");
      fail("Accepted negative k");
    } catch (final IllegalArgumentException e) {
      // expected
    }

    try {
      new VarOptSampling.Final("-1", "3");
      fail("Accepted negative k");
    } catch (final IllegalArgumentException e) {
      // expected
    }

    try {
      new VarOptSampling.Final("10", "-1");
      fail("Accepted negative weight index");
    } catch (final IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  public void standardAccumulate() {
    final int k = 10;
    final VarOptSampling udf = new VarOptSampling(Integer.toString(k), "0");

    final DataBag inputBag = BagFactory.getInstance().newDefaultBag();
    double cumWeight = 0.0;
    try {
      for (int i = 1; i < k; ++i) {
        final Tuple t = TupleFactory.getInstance().newTuple(3);
        t.set(0, 1.0 * i);
        t.set(1, i);
        t.set(2, -i);
        inputBag.add(t);
        cumWeight += i;
      }
      final Tuple inputTuple = TupleFactory.getInstance().newTuple(inputBag);

      assertNull(udf.getValue());
      udf.accumulate(inputTuple);
      udf.accumulate(inputTuple);
      final DataBag result = udf.getValue();
      udf.cleanup();
      assertNull(udf.getValue());

      assertNotNull(result);
      assertEquals(result.size(), k);
      double cumResultWeight = 0.0;
      for (Tuple weightAndtuple : result) {
        cumResultWeight += (double) weightAndtuple.get(0);
        final Tuple sample = (Tuple) weightAndtuple.get(1);
        assertEquals(sample.size(), 3);

        final int id = (int) sample.get(1);
        assertTrue((id  > 0) && (id < k));
      }
      assertEquals(cumResultWeight, 2 * cumWeight, EPS); // called accumulate() twice
    } catch (final IOException e) {
      fail("Unexpected exception");
    }
  }

  @Test
  public void degenerateExecInput() {
    final VarOptSampling udf = new VarOptSampling();

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
  public void algebraicDegenerateInput() {
    try {
      // Tuple version
      final VarOptSampling.Final udf;
      udf = new VarOptSampling.Final("4");
      DataBag result = udf.exec(null);
      assertNull(result);

      Tuple inputTuple = TupleFactory.getInstance().newTuple(0);
      result = udf.exec(inputTuple);
      assertNull(result);

      inputTuple = TupleFactory.getInstance().newTuple(1);
      inputTuple.set(0, null);
      result = udf.exec(inputTuple);
      assertNull(result);
    } catch (final IOException e) {
      fail("Unexpected IOException");
    }
  }

  @Test
  public void algebraicFinal() {
    final int k = 87;
    final int wtIdx = 2;

    final VarOptSampling.Final udf
            = new VarOptSampling.Final(Integer.toString(k),Integer.toString(wtIdx));

    final DataBag inputBag = BagFactory.getInstance().newDefaultBag();
    final VarOptItemsSketch<Tuple> vis = VarOptItemsSketch.newInstance(k);
    inputBag.add(TupleFactory.getInstance().newTuple(new DataByteArray(vis.toByteArray(serDe_))));

    final Tuple inputTuple = TupleFactory.getInstance().newTuple(inputBag);
    try {
      final DataBag result = udf.exec(inputTuple);
      assertNotNull(result);
      assertEquals(result.size(), 0);
    } catch (final IOException e) {
      fail("Unexpected IOException");
    }
  }

  @Test
  public void validOutputSchemaTest() throws IOException {
    VarOptSampling udf = new VarOptSampling("5", "1");

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
    assertEquals(output.getField(0).type, DataType.BAG);

    final List<Schema.FieldSchema> outputFields = output.getField(0).schema.getFields();
    assertEquals(outputFields.size(), 2);

    // check high-level structure
    assertEquals(outputFields.get(0).alias, WEIGHT_ALIAS);
    assertEquals(outputFields.get(0).type, DataType.DOUBLE);
    assertEquals(outputFields.get(1).alias, RECORD_ALIAS);
    assertEquals(outputFields.get(1).type, DataType.TUPLE);

    // validate sample bag
    final Schema sampleSchema = outputFields.get(1).schema;
    assertTrue(sampleSchema.equals(tupleSchema));

    // use the float as a weight instead
    udf = new VarOptSampling("5", "2");
    output = udf.outputSchema(inputSchema);
    assertEquals(output.size(), 1);
    assertEquals(output.getField(0).type, DataType.BAG);
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

    final VarOptSampling udf = new VarOptSampling("5", "0");

    // degenerate input schemas
    try {
      udf.outputSchema(null);
      fail("Accepted null schema");
    } catch (final IllegalArgumentException e) {
      // expected
    }

    try {
      udf.outputSchema(new Schema());
      fail("Accepted empty schema");
    } catch (final IllegalArgumentException e) {
      // expected
    }

    // expecting weight in element 0 (based on constructor args)
    try {
      udf.outputSchema(inputSchema);
      fail("Accepted non-weight in weightIndex column");
    } catch (final IllegalArgumentException e) {
      // expected
    }

    // passing in Tuple instead of DataBag
    try {
      udf.outputSchema(tupleSchema);
      fail("Accepted input Tuple instead of DataBag");
    } catch (final IllegalArgumentException e) {
      // expected
    }
  }
}
