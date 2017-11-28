package com.yahoo.sketches.pig.decomposition;

import static com.yahoo.sketches.pig.decomposition.DataToFrequentDirectionsSketchTest.createInputSamplesAndSketch;
import static com.yahoo.sketches.pig.decomposition.DataToFrequentDirectionsSketchTest.makeSketch;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.IOException;
import java.util.Random;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.decomposition.FrequentDirections;

public class MergeFrequentDirectionsTest {
  @Test
  public void degenerateExecInput() {
    final MergeFrequentDirections udf = new MergeFrequentDirections();

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
  public void checkExec() {
    checkAccumulateAndFinalExec(new MergeFrequentDirections());
  }

  // Algebraic.Initial
  @Test
  public void checkInitialExec() {
    final MergeFrequentDirections.Initial udf = new MergeFrequentDirections.Initial();

    assertNull(udf.exec(null)); // simple pass-through
    final Tuple input = TupleFactory.getInstance().newTuple("data string");
    final Tuple result = udf.exec(input);
    assertEquals(result, input); // simple pass-through
  }

  // Algebraic.Intermediate
  @Test
  public void checkDegenerateIntermediateExec() {
    final MergeFrequentDirections.Intermediate udf = new MergeFrequentDirections.Intermediate();

    final DataBag sketchBag = BagFactory.getInstance().newDefaultBag();
    final Tuple inputTuple = TupleFactory.getInstance().newTuple(sketchBag);

    try {
      assertNull(udf.exec(inputTuple)); // empty input bag

      final Tuple dataTuple = TupleFactory.getInstance().newTuple("data string");
      sketchBag.add(dataTuple);
      udf.exec(inputTuple);
      fail();
    } catch (final IllegalArgumentException e) {
      // expected
    } catch (IOException e) {
      fail("Unexpected exception");
    }
  }

  @Test
  public void checkIntermediateExec() {
    final int k = 5;
    final int d = 20;
    final int n = 13;

    final MergeFrequentDirections.Intermediate udf = new MergeFrequentDirections.Intermediate();

    try {
      final DataBag sketchBag = BagFactory.getInstance().newDefaultBag();
      final byte[] fd1 = makeSketch(k, d, n).toByteArray();
      final byte[] fd2 = makeSketch(k, d, n).toByteArray();
      final byte[] fd3 = makeSketch(k, d, n).toByteArray();
      final byte[] fd4 = makeSketch(k, d, n).toByteArray();

      sketchBag.add(TupleFactory.getInstance().newTuple(new DataByteArray(fd1)));
      sketchBag.add(TupleFactory.getInstance().newTuple(new DataByteArray(fd2)));
      sketchBag.add(TupleFactory.getInstance().newTuple(new DataByteArray(fd3)));
      sketchBag.add(TupleFactory.getInstance().newTuple(new DataByteArray(fd4)));

      // all items in a single bag
      Tuple inputTuple = TupleFactory.getInstance().newTuple(sketchBag);
      Tuple outputTuple = udf.exec(inputTuple);
      DataByteArray dbaResult = (DataByteArray) outputTuple.get(0);
      FrequentDirections result = FrequentDirections.heapify(Memory.wrap(dbaResult.get()));
      assertEquals(result.getN(), 4 * n);
      assertEquals(result.getK(), k);
      assertEquals(result.getD(), d);

      // have a couple inner bags
      final DataBag innerBag1 = BagFactory.getInstance().newDefaultBag();
      innerBag1.add(TupleFactory.getInstance().newTuple(new DataByteArray(fd1)));
      innerBag1.add(TupleFactory.getInstance().newTuple(new DataByteArray(fd2)));
      innerBag1.add(TupleFactory.getInstance().newTuple(new DataByteArray(fd3)));
      final DataBag innerBag2 = BagFactory.getInstance().newDefaultBag();
      innerBag2.add(TupleFactory.getInstance().newTuple(new DataByteArray(fd4)));

      final DataBag outerBag = BagFactory.getInstance().newDefaultBag();
      outerBag.add(TupleFactory.getInstance().newTuple(innerBag1));
      outerBag.add(TupleFactory.getInstance().newTuple(innerBag2));
      inputTuple = TupleFactory.getInstance().newTuple(outerBag);
      outputTuple = udf.exec(inputTuple);
      dbaResult = (DataByteArray) outputTuple.get(0);
      result = FrequentDirections.heapify(Memory.wrap(dbaResult.get()));
      assertEquals(result.getN(), 4 * n);
      assertEquals(result.getK(), k);
      assertEquals(result.getD(), d);
    } catch (final IOException e) {
      fail("Unexpected exception");
    }

  }

  // Algebraic.Final
  @Test
  public void checkFinalExec() {
    checkAccumulateAndFinalExec(new MergeFrequentDirections.Final());
  }

  // Main class's exec() and Final.exec() should behave identically
  private static void checkAccumulateAndFinalExec(final EvalFunc<DataByteArray> udf) {
    final int k = 5;
    final int d = 20;

    try {
      final FrequentDirections fd = createInputSamplesAndSketch(k, d, null);
      final byte[] fdBytes = fd.toByteArray();

      final DataBag sketchBag = BagFactory.getInstance().newDefaultBag();
      final Tuple inputTuple = TupleFactory.getInstance().newTuple(sketchBag);
      assertNull(udf.exec(inputTuple)); // with null bag

      final DataByteArray dba = new DataByteArray(fdBytes);
      final Tuple sketchTuple = TupleFactory.getInstance().newTuple(dba);
      sketchBag.add(sketchTuple);
      sketchBag.add(sketchTuple);

      final DataByteArray dbaResult = udf.exec(inputTuple);
      final FrequentDirections result = FrequentDirections.heapify(Memory.wrap(dbaResult.get()));

      assertEquals(result.getK(), k);
      assertEquals(result.getN(), 2 * fd.getN());

      sketchBag.clear();
      assertNull(udf.exec(inputTuple)); // if using cleanup(), should be empty rather than null this time

    } catch (final IOException e) {
      fail("Unexpected exception");
    }
  }
}
