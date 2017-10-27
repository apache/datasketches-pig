package com.yahoo.sketches.pig.decomposition;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;

import java.io.IOException;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.decomposition.FrequentDirections;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.testng.annotations.Test;

public class DataToSketchTest {
  // AccumulateEvalFunc
  @Test
  public void checkConstructors() {
    DataToSketch udf = new DataToSketch();
    assertNotNull(udf);

    udf = new DataToSketch("50", "100");
    assertNotNull(udf);

    try {
      new DataToSketch("-1", "3");
      fail("Accepted negative k");
    } catch (final IllegalArgumentException e) {
      // expected
    }

    try {
      new DataToSketch("10", "-1");
      fail("Accepted negative d");
    } catch (final IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  public void checkExecution() {
    final int k = 20;
    final int d = 50;
    final DataToSketch udf = new DataToSketch(Integer.toString(k), Integer.toString(d));

    final DataBag outerBag = BagFactory.getInstance().newDefaultBag();
    final Tuple inputTuple = TupleFactory.getInstance().newTuple(1);

    try {
      inputTuple.set(0, outerBag);
      udf.exec(inputTuple); // empty bag

      final FrequentDirections fd = FrequentDirections.newInstance(k, d);

      // add each value to both the input bag and a sketch
      for (int i = 1; i < k; ++i) {
        final DataBag innerBag = BagFactory.getInstance().newDefaultBag();
        final double[] vector = new double[d];
        // set 2 points, so 2 Tuples
        Tuple t = TupleFactory.getInstance().newTuple(2);
        t.set(0, i);
        t.set(1, 1.0 * i);
        innerBag.add(t);
        vector[i] = 1.0 * i;

        t = TupleFactory.getInstance().newTuple(2);
        t.set(0, k + i);
        t.set(1, 1.0 * i);
        innerBag.add(t);
        vector[k + i] = 1.0 * i;

        final Tuple record = TupleFactory.getInstance().newTuple(innerBag);
        outerBag.add(record);
        fd.update(vector);
      }

      assertNull(udf.getValue());
      udf.accumulate(inputTuple);
      final DataByteArray outBytes = udf.getValue();
      udf.cleanup();
      assertNull(udf.getValue());

      final FrequentDirections result = FrequentDirections.heapify(Memory.wrap(outBytes.get()));

      assertNotNull(result);
      assertEquals(result.getK(), fd.getK());
      assertEquals(result.getD(), fd.getD());
      assertEquals(result.getN(), fd.getN());
      assertEquals(result.getSingularValues(), fd.getSingularValues());
    } catch (final IOException e) {
      fail("Unexpected exception");
    }
  }

  @Test
  public void degenerateExecInput() {
    final DataToSketch udf = new DataToSketch();

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

  // Algebraic.Initial
  @Test
  public void checkInitialExec() {
    DataToSketch.Initial udf = new DataToSketch.Initial("5", "10");

    assertNull(udf.exec(null));
    Tuple input = TupleFactory.getInstance().newTuple("data string");
    Tuple result = udf.exec(input);
    assertEquals(result, input); // simple pass-through
  }

  // Algebraic.Intermediate
  @Test
  public void checkIntermediateConstructors() {
    DataToSketch.Intermediate udf = new DataToSketch.Intermediate();
    assertNotNull(udf);

    udf = new DataToSketch.Intermediate("50", "100");
    assertNotNull(udf);

    try {
      new DataToSketch.Intermediate("-1", "3");
      fail("Accepted negative k");
    } catch (final IllegalArgumentException e) {
      // expected
    }

    try {
      new DataToSketch.Intermediate("10", "-1");
      fail("Accepted negative d");
    } catch (final IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  public void degenerateIntermediateExecInput() {
    final DataToSketch.Intermediate udf = new DataToSketch.Intermediate();

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

  // Algebraic.Final
  @Test
  public void checkFinalConstructors() {
    DataToSketch.Final udf = new DataToSketch.Final();
    assertNotNull(udf);

    udf = new DataToSketch.Final("50", "100");
    assertNotNull(udf);

    try {
      new DataToSketch.Final("-1", "3");
      fail("Accepted negative k");
    } catch (final IllegalArgumentException e) {
      // expected
    }

    try {
      new DataToSketch.Final("10", "-1");
      fail("Accepted negative d");
    } catch (final IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  public void checkFinalExec() {
    final int k = 5;
    final int d = 20;

    final FrequentDirections fd = FrequentDirections.newInstance(k, d);
    final double[] vector = new double[d];
    // add 2 vectors
    vector[0] = 1.0;
    fd.update(vector);

    vector[d - 1] = 1.0;
    fd.update(vector);

    byte[] fdBytes = fd.toByteArray();

    try {
      final DataByteArray dba = new DataByteArray(fdBytes);
      final Tuple sketchTuple = TupleFactory.getInstance().newTuple(dba);
      final DataBag sketchBag = BagFactory.getInstance().newDefaultBag();
      sketchBag.add(sketchTuple);
      sketchBag.add(sketchTuple);

      final Tuple inputTuple = TupleFactory.getInstance().newTuple(sketchBag);

      final DataToSketch.Final udf = new DataToSketch.Final(Integer.toString(k), Integer.toString(d));
      final DataByteArray dbaResult = udf.exec(inputTuple);
      final FrequentDirections result = FrequentDirections.heapify(Memory.wrap(dbaResult.get()));

      assertEquals(result.getK(), k);
      assertEquals(result.getN(), 4); // 2 entries, and sent in 2 copies of the sketch
    } catch (final IOException e) {
      fail("Unexpected exception");
    }
  }

  @Test
  public void degenerateFinalExecInput() {
    final DataToSketch.Final udf = new DataToSketch.Final();

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
}
