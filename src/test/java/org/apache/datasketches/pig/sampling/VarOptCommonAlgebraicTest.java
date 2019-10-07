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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.IOException;
import java.util.HashMap;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.testng.annotations.Test;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.sampling.VarOptItemsSamples;
import org.apache.datasketches.sampling.VarOptItemsSketch;
import org.apache.datasketches.sampling.VarOptItemsUnion;

@SuppressWarnings("javadoc")
public class VarOptCommonAlgebraicTest {
  private static final ArrayOfTuplesSerDe serDe_ = new ArrayOfTuplesSerDe();

  // constructors: just make sure result not null with valid args, throw exceptions if invalid
  @SuppressWarnings("unused")
  @Test
  public void rawTuplesToSketchConstructors() {
    VarOptCommonImpl.RawTuplesToSketchTuple udf;

    udf = new VarOptCommonImpl.RawTuplesToSketchTuple();
    assertNotNull(udf);

    udf = new VarOptCommonImpl.RawTuplesToSketchTuple("5");
    assertNotNull(udf);

    udf = new VarOptCommonImpl.RawTuplesToSketchTuple("5", "3");
    assertNotNull(udf);

    try {
      new VarOptCommonImpl.RawTuplesToSketchTuple("-1");
      fail("Accepted negative k");
    } catch (final IllegalArgumentException e) {
      // expected
    }

    try {
      new VarOptCommonImpl.RawTuplesToSketchTuple("-1", "3");
      fail("Accepted negative k");
    } catch (final IllegalArgumentException e) {
      // expected
    }

    try {
      new VarOptCommonImpl.RawTuplesToSketchTuple("10", "-1");
      fail("Accepted negative weight index");
    } catch (final IllegalArgumentException e) {
      // expected
    }
  }

  @SuppressWarnings("unused")
  @Test
  public void unionSketchesAsSketchConstructors() {
    VarOptCommonImpl.UnionSketchesAsTuple udf;

    udf = new VarOptCommonImpl.UnionSketchesAsTuple();
    assertNotNull(udf);

    udf = new VarOptCommonImpl.UnionSketchesAsTuple("5");
    assertNotNull(udf);

    udf = new VarOptCommonImpl.UnionSketchesAsTuple("5", "3");
    assertNotNull(udf);

    try {
      new VarOptCommonImpl.UnionSketchesAsTuple("-1");
      fail("Accepted negative k");
    } catch (final IllegalArgumentException e) {
      // expected
    }

    try {
      new VarOptCommonImpl.UnionSketchesAsTuple("-1", "3");
      fail("Accepted negative k");
    } catch (final IllegalArgumentException e) {
      // expected
    }

    try {
      new VarOptCommonImpl.UnionSketchesAsTuple("10", "-1");
      fail("Accepted negative weight index");
    } catch (final IllegalArgumentException e) {
      // expected
    }
  }

  @SuppressWarnings("unused")
  @Test
  public void unionSketchesAsByteArrayConstructors() {
    VarOptCommonImpl.UnionSketchesAsByteArray udf;

    udf = new VarOptCommonImpl.UnionSketchesAsByteArray();
    assertNotNull(udf);

    udf = new VarOptCommonImpl.UnionSketchesAsByteArray("5");
    assertNotNull(udf);

    udf = new VarOptCommonImpl.UnionSketchesAsByteArray("5", "3");
    assertNotNull(udf);

    try {
      new VarOptCommonImpl.UnionSketchesAsByteArray("-1");
      fail("Accepted negative k");
    } catch (final IllegalArgumentException e) {
      // expected
    }

    try {
      new VarOptCommonImpl.UnionSketchesAsByteArray("-1", "3");
      fail("Accepted negative k");
    } catch (final IllegalArgumentException e) {
      // expected
    }

    try {
      new VarOptCommonImpl.UnionSketchesAsByteArray("10", "-1");
      fail("Accepted negative weight index");
    } catch (final IllegalArgumentException e) {
      // expected
    }
  }

  // exec: sketches generally in sampling mode
  @Test
  public void rawTuplesToSketchTupleExec() {
    final int k = 5;
    final int wtIdx = 1;
    final VarOptCommonImpl.RawTuplesToSketchTuple udf;

    udf = new VarOptCommonImpl.RawTuplesToSketchTuple(Integer.toString(k), Integer.toString(wtIdx));

    char id = 'a';
    double wt = 1.0;

    final DataBag inputBag = BagFactory.getInstance().newDefaultBag();
    try {
      for (int i = 0; i < (k + 1); ++i) {
        final Tuple t = TupleFactory.getInstance().newTuple(2);
        t.set(0, Character.toString(id));
        t.set(1, wt);
        inputBag.add(t);

        ++id;
        wt += 1.0;
      }
    } catch (final ExecException e) {
      fail("Unexpected ExecException creating input data");
    }

    try {
      // degenerate input first
      Tuple result = udf.exec(null);
      assertNull(result);

      Tuple inputTuple = TupleFactory.getInstance().newTuple(0);
      result = udf.exec(inputTuple);
      assertNull(result);

      inputTuple = TupleFactory.getInstance().newTuple(1);
      inputTuple.set(0, null);
      result = udf.exec(inputTuple);
      assertNull(result);

      // now test real input
      inputTuple.set(0, inputBag);
      result = udf.exec(inputTuple);
      assertEquals(result.size(), 1);
      final DataByteArray dba = (DataByteArray) result.get(0);

      final VarOptItemsSketch<Tuple> vis;
      vis = VarOptItemsSketch.heapify(Memory.wrap(dba.get()), serDe_);
      assertEquals(vis.getN(), k + 1);
      assertEquals(vis.getK(), k);

      // just validating the original weights are within the expected range
      for (VarOptItemsSamples<Tuple>.WeightedSample ws : vis.getSketchSamples()) {
        final Tuple t = ws.getItem();
        assertTrue((double) t.get(wtIdx) >= 1.0);
        assertTrue((double) t.get(wtIdx) <= (k + 1.0));
      }
    } catch (final IOException e) {
      fail("Unexpected IOException calling exec()");
    }
  }

  @Test
  public void unionSketchesDegenerateInput() {
    try {
      // Tuple version
      final VarOptCommonImpl.UnionSketchesAsTuple udfTuple;
      udfTuple = new VarOptCommonImpl.UnionSketchesAsTuple("4");
      Tuple result = udfTuple.exec(null);
      assertNull(result);

      Tuple inputTuple = TupleFactory.getInstance().newTuple(0);
      result = udfTuple.exec(inputTuple);
      assertNull(result);

      inputTuple = TupleFactory.getInstance().newTuple(1);
      inputTuple.set(0, null);
      result = udfTuple.exec(inputTuple);
      assertNull(result);

      // DataByteArray version
      final VarOptCommonImpl.UnionSketchesAsByteArray udfBA;
      udfBA = new VarOptCommonImpl.UnionSketchesAsByteArray("4");
      DataByteArray output = udfBA.exec(null);
      assertNull(output);

      inputTuple = TupleFactory.getInstance().newTuple(0);
      output = udfBA.exec(inputTuple);
      assertNull(output);

      inputTuple = TupleFactory.getInstance().newTuple(1);
      inputTuple.set(0, null);
      output = udfBA.exec(inputTuple);
      assertNull(output);
    } catch (final IOException e) {
      fail("Unexpected IOException calling exec()");
    }
  }

  @Test
  public void unionSketchesExec() {
    // Only difference between UnionSketchesAsTuple and UnionSketchesAsByteArray is that one wraps
    // the resulting serialized sketch in a tuple. If the union result is still in exact mode, the
    // two sketches should be identical.
    final int numSketches = 3;
    final int numItemsPerSketch = 10; // numSketches * numItemsPerSketch should be < k here
    final int k = 100;
    final String kStr = Integer.toString(k);
    final VarOptCommonImpl.UnionSketchesAsTuple udfTuple;
    final VarOptCommonImpl.UnionSketchesAsByteArray udfBA;

    udfTuple = new VarOptCommonImpl.UnionSketchesAsTuple(kStr);
    udfBA = new VarOptCommonImpl.UnionSketchesAsByteArray(kStr);

    char id = 'a';
    double wt = 1.0;

    final DataBag inputBag = BagFactory.getInstance().newDefaultBag();
    final VarOptItemsUnion<Tuple> union = VarOptItemsUnion.newInstance(k);
    final VarOptItemsSketch<Tuple> vis = VarOptItemsSketch.newInstance(k);

    // Create numSketches VarOpt sketches and serialize them. Also create a standard union to
    // compare against at the end.
    try {
      for (int j = 0; j < numSketches; ++j) {
        vis.reset();
        for (int i = 0; i < numItemsPerSketch; ++i) {
          final Tuple t = TupleFactory.getInstance().newTuple(2);
          t.set(0, Character.toString(id));
          t.set(1, wt);
          vis.update(t, wt);

          ++id;
          wt += 1.0;
        }

        final Tuple wrapper = TupleFactory.getInstance().newTuple(1);
        wrapper.set(0, new DataByteArray(vis.toByteArray(serDe_)));
        inputBag.add(wrapper);
        union.update(vis);
      }
    } catch (final ExecException e) {
      fail("Unexpected ExecException creating input data");
    }

    try {
      final Tuple inputTuple = TupleFactory.getInstance().newTuple(1);
      inputTuple.set(0, inputBag);

      final DataByteArray outArray = udfBA.exec(inputTuple);
      final VarOptItemsSketch<Tuple> sketch1
              = VarOptItemsSketch.heapify(Memory.wrap(outArray.get()), serDe_);

      final Tuple outTuple = udfTuple.exec(inputTuple);
      final DataByteArray dba = (DataByteArray) outTuple.get(0);
      final VarOptItemsSketch<Tuple> sketch2
              = VarOptItemsSketch.heapify(Memory.wrap(dba.get()), serDe_);

      final VarOptItemsSketch<Tuple> expectedResult = union.getResult();
      compareResults(sketch1, expectedResult);
      compareResults(sketch2, expectedResult);
    } catch (final IOException e) {
      fail("Unexpected IOException calling exec()");
    }
  }

  // checks N, K, and ensures the same weights and items exist (order may differ)
  static void compareResults(final VarOptItemsSketch<Tuple> s1,
                      final VarOptItemsSketch<Tuple> s2) {
    assertEquals(s1.getN(), s2.getN());
    assertEquals(s1.getK(), s2.getK());

    final HashMap<Tuple, Double> items = new HashMap<>(s1.getNumSamples());
    for (VarOptItemsSamples<Tuple>.WeightedSample ws : s1.getSketchSamples()) {
      items.put(ws.getItem(), ws.getWeight());
    }

    for (VarOptItemsSamples<Tuple>.WeightedSample ws : s2.getSketchSamples()) {
      assertEquals(items.get(ws.getItem()), ws.getWeight());
    }
  }


}
