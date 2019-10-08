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
import static org.apache.datasketches.pig.sampling.ReservoirSampling.SAMPLES_ALIAS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.IOException;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.testng.annotations.Test;

@SuppressWarnings("javadoc")
public class ReservoirSamplingTest {

  @SuppressWarnings("unused")
  @Test
  public void invalidKTest() {
    try {
      new ReservoirSampling("1");
      fail();
    } catch (final IllegalArgumentException e) {
      // expected
    }

    try {
      new ReservoirSampling.Initial("1");
      fail();
    } catch (final IllegalArgumentException e) {
      // expected
    }

    try {
      new ReservoirSampling.IntermediateFinal("1");
      fail();
    } catch (final IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  public void accumulateTest() throws IOException {
    // exec() is automatically composed by calling accumulate(), getValue(), and cleanup(), in order
    // since AccumulateEvalFunc, but includes a fast-return route so still need to test separately
    final int k = 32;
    final long n = 24;
    final DataBag inputBag = BagFactory.getInstance().newDefaultBag();

    final TupleFactory tf = TupleFactory.getInstance();
    for (long i = 0; i < n; ++i) {
      final Tuple t = tf.newTuple(2);
      t.set(0, i);
      t.set(1, Long.toString(-i));
      inputBag.add(t);
    }

    final Tuple input = tf.newTuple(inputBag);
    final ReservoirSampling rs = new ReservoirSampling(Integer.toString(k));
    rs.accumulate(input);
    Tuple result = rs.getValue();

    assertEquals(result.size(), 3, "Incorrect output size");
    assertEquals(result.get(0), n, "Incorrect number of samples seen");
    assertEquals(result.get(1), k, "Incorrect value of k");
    assertEquals(((DataBag) result.get(2)).size(), n);

    // run the same input through again
    rs.accumulate(input);
    result = rs.getValue();
    assertEquals(result.get(0), 2 * n, "Incorrect number of samples seen");
    assertEquals(result.get(1), k, "Incorrect value of k"); // unchanged
    assertEquals(((DataBag) result.get(2)).size(), Math.min(k, 2 * n));

    // clean up, degenerate accumulate, then get value again
    rs.cleanup();
    rs.accumulate(null);
    assertNull(rs.getValue());
  }

  @Test
  public void execTest() throws IOException {
    // copies tests for accumulate() since that handles both data paths
    final int k = 32;
    final long n = 24;
    final DataBag inputBag = BagFactory.getInstance().newDefaultBag();

    final TupleFactory tf = TupleFactory.getInstance();
    for (long i = 0; i < n; ++i) {
      final Tuple t = tf.newTuple(2);
      t.set(0, i);
      t.set(1, Long.toString(-i));
      inputBag.add(t);
    }

    final Tuple input = tf.newTuple(inputBag);
    final ReservoirSampling rs = new ReservoirSampling(Integer.toString(k));
    Tuple result = rs.exec(input);

    assertEquals(result.size(), 3, "Incorrect output size");
    assertEquals(result.get(0), n, "Incorrect number of samples seen");
    assertEquals(result.get(1), k, "Incorrect value of k");
    assertEquals(((DataBag) result.get(2)).size(), n);

    // add another n to the bag and repeat
    for (long i = n; i < (2 * n); ++i) {
      final Tuple t = tf.newTuple(2);
      t.set(0, i);
      t.set(1, Long.toString(-i));
      inputBag.add(t);
    }
    result = rs.exec(input);
    assertEquals(result.get(0), 2 * n, "Incorrect number of samples seen");
    assertEquals(result.get(1), k, "Incorrect value of k"); // unchanged
    assertEquals(((DataBag) result.get(2)).size(), Math.min(k, 2 * n));
  }

  @Test
  public void initialExec() throws IOException {
    final int k = 32;
    final long n1 = 16;
    final long n2 = 64;

    final ReservoirSampling.Initial rs = new ReservoirSampling.Initial(Integer.toString(k));

    // 2 cases: n <= k and n > k
    final DataBag inputBag = BagFactory.getInstance().newDefaultBag();

    final TupleFactory tf = TupleFactory.getInstance();
    for (long i = 0; i < n1; ++i) {
      final Tuple t = tf.newTuple(2);
      t.set(0, i);
      t.set(1, Long.toString(-i));
      inputBag.add(t);
    }

    final Tuple input = tf.newTuple(inputBag);
    Tuple result = rs.exec(input);

    assertEquals(result.size(), 3,  "Incorrect output size");
    assertEquals(result.get(0), n1, "Incorrect number of samples seen");
    assertEquals(result.get(1), k,  "Incorrect value of k");
    assertEquals(((DataBag) result.get(2)).size(), n1);

    // add so bag has n2 values
    for (long i = n1; i < n2; ++i) {
      final Tuple t = tf.newTuple(2);
      t.set(0, i);
      t.set(1, Long.toString(-i));
      inputBag.add(t);
    }

    // inputBag should already be part of input tuple
    result = rs.exec(input);

    assertEquals(result.size(), 3,  "Incorrect output size");
    assertEquals(result.get(0), n2, "Incorrect number of samples seen");
    assertEquals(result.get(1), k,  "Incorrect value of k");
    assertEquals(((DataBag) result.get(2)).size(), k);
  }

  @Test
  public void intermediateFinalExec() throws IOException {
    final int maxK = 128;

    final EvalFunc<Tuple> rs = new ReservoirSampling.IntermediateFinal(Integer.toString(maxK));

    // need at least 3 conditions:
    // 1. n <= k <= maxK
    // 2. n <= k, k > maxK
    // 3. n > k
    final DataBag bagOfReservoirs = BagFactory.getInstance().newDefaultBag();

    Tuple t = TupleFactory.getInstance().newTuple(3);
    t.set(0, 32L);
    t.set(1, maxK);
    t.set(2, generateDataBag(32, 0));
    bagOfReservoirs.add(t);

    t = TupleFactory.getInstance().newTuple(3);
    t.set(0, 64L);
    t.set(1, 256);
    t.set(2, generateDataBag(64, 32));
    bagOfReservoirs.add(t);

    t = TupleFactory.getInstance().newTuple(3);
    t.set(0, 256L);
    t.set(1, maxK);
    t.set(2, generateDataBag(maxK, 96));
    bagOfReservoirs.add(t);

    final Tuple input = TupleFactory.getInstance().newTuple(1);
    input.set(0, bagOfReservoirs);

    final Tuple result = rs.exec(input);
    final long tgtN = 32 + 64 + 256;
    final int tgtMaxVal = 32 + 64 + maxK; // only added maxK to last bag
    assertEquals(result.size(), 3,  "Incorrect output size");
    assertEquals(result.get(0), tgtN, "Incorrect number of samples seen");
    assertEquals(result.get(1), maxK,  "Incorrect value of k");
    assertEquals(((DataBag) result.get(2)).size(), maxK);

    // check that they're all in the target range
    for (Tuple sample : ((DataBag) result.get(2))) {
      final int val = (int) sample.get(0);
      if ((val < 0) || (val >= tgtMaxVal)) {
        fail("Found value (" + val + ") outside target range [0, " + tgtMaxVal + "]");
      }
    }
  }

  @Test
  public void outputSchemaTest() throws IOException {
    final ReservoirSampling rs = new ReservoirSampling("5");

    final Schema recordSchema = new Schema();
    recordSchema.add(new Schema.FieldSchema("field1", DataType.CHARARRAY));
    recordSchema.add(new Schema.FieldSchema("field2", DataType.INTEGER));
    final Schema tupleSchema = new Schema();
    tupleSchema.add(new Schema.FieldSchema("record", recordSchema, DataType.TUPLE));

    final Schema inputSchema = new Schema();
    inputSchema.add(new Schema.FieldSchema("data", tupleSchema, DataType.BAG));

    final Schema output = rs.outputSchema(inputSchema);
    assertEquals(output.size(), 1);

    final List<Schema.FieldSchema> outputFields = output.getField(0).schema.getFields();
    assertEquals(outputFields.size(), 3);

    // check high-level structure
    assertEquals(outputFields.get(0).alias, N_ALIAS);
    assertEquals(outputFields.get(0).type, DataType.LONG);
    assertEquals(outputFields.get(1).alias, K_ALIAS);
    assertEquals(outputFields.get(1).type, DataType.INTEGER);
    assertEquals(outputFields.get(2).alias, SAMPLES_ALIAS);
    assertEquals(outputFields.get(2).type, DataType.BAG);

    // validate sample bag
    final Schema sampleSchema = outputFields.get(2).schema;
    assertTrue(sampleSchema.equals(tupleSchema));
  }

  @Test
  public void degenerateAccumulateInput() {
    final ReservoirSampling rs = new ReservoirSampling("256");

    // all these tests should do nothing
    try {
      rs.accumulate(null);

      Tuple input = TupleFactory.getInstance().newTuple(0);
      rs.accumulate(input);

      input = TupleFactory.getInstance().newTuple(1);
      input.set(0, null);
      rs.accumulate(input);
    } catch (final IOException e) {
      fail("Unexpected IOException: " + e.getMessage());
    }
  }

  @Test
  public void degenerateExecInput() {
    final ReservoirSampling rs = new ReservoirSampling("256");

    // all these tests should do nothing
    try {
      rs.exec(null);

      Tuple input = TupleFactory.getInstance().newTuple(0);
      rs.exec(input);

      input = TupleFactory.getInstance().newTuple(1);
      input.set(0, null);
      rs.exec(input);
    } catch (final IOException e) {
      fail("Unexpected IOException: " + e.getMessage());
    }
  }

  @Test
  public void degenerateInitialInput() {
    try {
      final ReservoirSampling.Initial rs = new ReservoirSampling.Initial("256");

      rs.exec(null);

      Tuple input = TupleFactory.getInstance().newTuple(0);
      rs.exec(input);

      input = TupleFactory.getInstance().newTuple(1);
      input.set(0, null);
      rs.exec(input);
    } catch (final IOException e) {
      fail("Unexpected IOException: " + e.getMessage());
    }
  }

  @Test
  public void degenerateIntermediateFinalInput() {
    try {
      final EvalFunc<Tuple> rs = new ReservoirSampling.IntermediateFinal("256");

      rs.exec(null);

      Tuple input = TupleFactory.getInstance().newTuple(0);
      rs.exec(input);

      input = TupleFactory.getInstance().newTuple(1);
      input.set(0, null);
      rs.exec(input);
    } catch (final IOException e) {
      fail("Unexpected IOException: " + e.getMessage());
    }
  }

  @Test
  public void degenerateSchemaTest() {
    final ReservoirSampling rs = new ReservoirSampling("5");
    Schema output = rs.outputSchema(null);
    assertNull(output);

    output = rs.outputSchema(new Schema());
    assertNull(output);
  }

  static DataBag generateDataBag(final long numItems, final int startIdx) {
    final DataBag output = BagFactory.getInstance().newDefaultBag();

    try {
      for (int i = 0; i < numItems; ++i) {
        final Tuple t = TupleFactory.getInstance().newTuple(2);
        final int val = startIdx + i;
        t.set(0, val);
        t.set(1, Integer.toString(-val));
        output.add(t);
      }
    } catch (final ExecException e) {
      fail(e.getMessage());
    }

    return output;
  }
}
