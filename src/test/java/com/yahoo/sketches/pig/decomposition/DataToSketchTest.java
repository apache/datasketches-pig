package com.yahoo.sketches.pig.decomposition;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.IOException;
import java.util.Random;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.decomposition.FrequentDirections;

public class DataToSketchTest {
  private static final Random rand = new Random();

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

      final FrequentDirections fd = createInputSamplesAndSketch(k, d, outerBag);

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
    final DataToSketch.Intermediate udf = new DataToSketch.Intermediate("5", "15");

    try {
      assertNull(udf.exec(null));
      assertNull(udf.exec(TupleFactory.getInstance().newTuple(0)));

      final Tuple in = TupleFactory.getInstance().newTuple(1);
      in.set(0, null);
      assertNull(udf.exec(in));

      in.set(0, BagFactory.getInstance().newDefaultBag());
      assertNull(udf.exec(in));
    } catch (final IOException e) {
      fail("Unexpected exception");
    }

    try {
      final DataBag outerBag = BagFactory.getInstance().newDefaultBag();
      outerBag.add(TupleFactory.getInstance().newTuple("test string"));
      udf.exec(TupleFactory.getInstance().newTuple(outerBag));
      fail();
    } catch (final IllegalArgumentException e) {
      // expected
    } catch (final IOException e) {
      fail("Unexpected exception");
    }
  }

  @Test
  public void checkIntermediateDataBagExec() {
    final int k = 12;
    final int d = 30;

    DataToSketch.Intermediate udf = new DataToSketch.Intermediate(Integer.toString(k), Integer.toString(d));

    DataByteArray dba = new DataByteArray(makeSketch(k, d, d).toByteArray());
    Tuple sketchTuple = TupleFactory.getInstance().newTuple(dba);
    DataBag outerBag = BagFactory.getInstance().newDefaultBag();
    outerBag.add(sketchTuple);

    try {
      // input as DataBag of serialized sketches
      final Tuple inputTuple = TupleFactory.getInstance().newTuple(1);
      inputTuple.set(0, outerBag);
      udf.exec(inputTuple);

      // input as Bag of Bags of serialized sketches
      DataBag innerBag = BagFactory.getInstance().newDefaultBag();
      dba = new DataByteArray(makeSketch(k, d, d).toByteArray());
      sketchTuple = TupleFactory.getInstance().newTuple(dba);
      innerBag.add(sketchTuple);
      dba = new DataByteArray(makeSketch(k, d, d).toByteArray());
      sketchTuple = TupleFactory.getInstance().newTuple(dba);
      innerBag.add(sketchTuple);

      udf.exec(inputTuple);
    } catch (final IOException e) {
      fail("Unexpected exception");
    }
  }

  @Test
  public void checkIntermediateTupleExec() {
    final int k = 20;
    final int d = 50;
    final DataToSketch.Intermediate udf = new DataToSketch.Intermediate(Integer.toString(k), Integer.toString(d));

    final DataBag outerBag = BagFactory.getInstance().newDefaultBag();
    final Tuple inputTuple = TupleFactory.getInstance().newTuple(1);

    try {
      inputTuple.set(0, outerBag);
      udf.exec(inputTuple); // empty bag

      final FrequentDirections fd = createInputSamplesAndSketch(k, d, outerBag);
      final Tuple outTuple = udf.exec(inputTuple);
      assertNotNull(outTuple);
      assertEquals(outTuple.size(), 1);
      assertTrue(outTuple.get(0) instanceof DataByteArray);

      final DataByteArray outBytes = (DataByteArray) outTuple.get(0);
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

  private static FrequentDirections makeSketch(final int k, final int d, final int n) {
    final FrequentDirections fd = FrequentDirections.newInstance(k, d);

    // create some noisy data that approximates a linear increase along the diagonal
    final double[] vector = new double[d];
    for (int i = 0; i < n; ++i) {
      for (int j = 0; j < d; ++j) {
        double val = rand.nextGaussian();
        if (i == j) { val += 2 * j * rand.nextDouble(); }
        vector[j] = val;
      }
      fd.update(vector);
    }

    return fd;
  }

  private static FrequentDirections createInputSamplesAndSketch(final int k, final int d,
                                                                final DataBag outerBag) throws ExecException {
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

    return fd;
  }


}
