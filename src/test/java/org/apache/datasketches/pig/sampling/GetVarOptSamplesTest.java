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
import static org.apache.datasketches.pig.sampling.VarOptSamplingTest.EPS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.IOException;
import java.util.List;

import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.testng.annotations.Test;

import org.apache.datasketches.sampling.VarOptItemsSketch;

@SuppressWarnings("javadoc")
public class GetVarOptSamplesTest {
  private static final ArrayOfTuplesSerDe serDe_ = new ArrayOfTuplesSerDe();

  @Test
  public void checkDegenerateInput() {
    final GetVarOptSamples udf = new GetVarOptSamples();

    try {
      assertNull(udf.exec(null));
      assertNull(udf.exec(TupleFactory.getInstance().newTuple(0)));
      assertNull(udf.exec(TupleFactory.getInstance().newTuple((Object) null)));
    } catch (final IOException e) {
      fail("Unexpected IOException");
    }
  }

  @Test
  public void checkExec() {
    final int k = 10;
    final int n = 25;  // exact mode
    final GetVarOptSamples udf = new GetVarOptSamples();

    try {
      final VarOptItemsSketch<Tuple> vis = VarOptItemsSketch.newInstance(k);
      double cumWt = 0.0;
      for (int i = 1; i <= n; ++i) {
        final Tuple t = TupleFactory.getInstance().newTuple(2);
        final double wt = 1.0 * i;
        t.set(0, wt);
        t.set(1, i);
        vis.update(t, wt);
        cumWt += wt;
      }

      final DataByteArray dba = new DataByteArray(vis.toByteArray(serDe_));
      final Tuple inputTuple = TupleFactory.getInstance().newTuple(dba);
      final DataBag result = udf.exec(inputTuple);

      double cumResultWt = 0.0;
      for (Tuple sample : result) {
        cumResultWt += (double) sample.get(0);
        final Tuple record = (Tuple) sample.get(1);
        final int id = (int) record.get(1);
        assertTrue((id >= 1) && (id <= n));
      }
      assertEquals(cumResultWt, cumWt, EPS);
    } catch (final IOException e) {
      fail("Unexpected IOException" + e.getMessage());
    }
  }

  @Test
  public void validOutputSchemaTest() {
    final GetVarOptSamples udf = new GetVarOptSamples();

    try {
      final Schema serializedSketch = new Schema();
      serializedSketch.add(new Schema.FieldSchema("field1", DataType.BYTEARRAY));

      final Schema output = udf.outputSchema(serializedSketch);
      assertEquals(output.size(), 1);
      assertEquals(output.getField(0).type, DataType.BAG);

      final List<Schema.FieldSchema> outputFields = output.getField(0).schema.getFields();
      assertEquals(outputFields.size(), 2);

      // check high-level structure
      assertEquals(outputFields.get(0).alias, WEIGHT_ALIAS);
      assertEquals(outputFields.get(0).type, DataType.DOUBLE);
      assertEquals(outputFields.get(1).alias, RECORD_ALIAS);
      assertEquals(outputFields.get(1).type, DataType.TUPLE);
    } catch (final IOException e) {
      fail("Unexpected IOException: " + e.getMessage());
    }
  }

  @Test
  public void badOutputSchemaTest() {
    final GetVarOptSamples udf = new GetVarOptSamples();

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

    try {
      final Schema wrongSchema = new Schema();
      wrongSchema.add(new Schema.FieldSchema("field", DataType.BOOLEAN));
      udf.outputSchema(wrongSchema);
      fail("Accepted schema with no DataByteArray");
    } catch (final IllegalArgumentException e) {
      // expected
    }
  }
}
