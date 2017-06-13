package com.yahoo.sketches.pig.sampling;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.sampling.VarOptItemsSamples;
import com.yahoo.sketches.sampling.VarOptItemsSketch;
import com.yahoo.sketches.sampling.VarOptItemsUnion;

/**
 * @author Jon Malkin
 */
class VarOptCommonImpl {
  static final int DEFAULT_TARGET_K = 1024;

  static final String WEIGHT_ALIAS = "vo_weight";
  static final String RECORD_ALIAS = "record";

  private static final BagFactory BAG_FACTORY = BagFactory.getInstance();
  private static final TupleFactory TUPLE_FACTORY = TupleFactory.getInstance();
  private static final ArrayOfTuplesSerDe SERDE = new ArrayOfTuplesSerDe();

  static VarOptItemsSketch<Tuple> rawTuplesToSketch(final Tuple inputTuple, final int k)
          throws IOException {
    assert inputTuple != null;
    assert inputTuple.size() >= 1;
    assert !inputTuple.isNull(0);

    final DataBag samples = (DataBag) inputTuple.get(0);
    final VarOptItemsSketch<Tuple> sketch = VarOptItemsSketch.newInstance(k);

    for (Tuple t : samples) {
      // first element is weight
      final double weight = (double) t.get(0);
      sketch.update(t, weight);
    }

    return sketch;
  }

  static VarOptItemsUnion<Tuple> unionSketches(final Tuple inputTuple, final int k)
          throws IOException {
    assert inputTuple != null;
    assert inputTuple.size() >= 1;
    assert !inputTuple.isNull(0);

    final VarOptItemsUnion<Tuple> union = VarOptItemsUnion.newInstance(k);

    final DataBag sketchBag = (DataBag) inputTuple.get(0);
    for (Tuple t : sketchBag) {
      final DataByteArray dba = (DataByteArray) t.get(0);
      final Memory mem = Memory.wrap(dba.get());
      union.update(mem, SERDE);
    }

    return union;
  }

  static Tuple wrapSketchInTuple(final VarOptItemsSketch<Tuple> sketch) throws IOException {
    final DataByteArray dba = new DataByteArray(sketch.toByteArray(SERDE));
    final Tuple outputTuple = TUPLE_FACTORY.newTuple(1);
    outputTuple.set(0, dba);
    return outputTuple;
  }

  static DataBag createResultFromSketch(final VarOptItemsSketch<Tuple> sketch) {
    final DataBag output = BAG_FACTORY.newDefaultBag();

    final VarOptItemsSamples<Tuple> samples = sketch.getSketchSamples();

    try {
      // create (weight, item) tuples to add to output bag
      for (VarOptItemsSamples.WeightedSample ws : samples) {
        final Tuple weightedSample = TUPLE_FACTORY.newTuple(2);
        weightedSample.set(0, ws.getWeight());
        weightedSample.set(1, ws.getItem());
        output.add(weightedSample);
      }
    } catch (final ExecException e) {
      throw new RuntimeException("Pig error: " + e.getMessage(), e);
    }

    return output;
  }

  public static class RawTuplesToSketchTupleImpl extends EvalFunc<Tuple> {
    private final int targetK_;

    public RawTuplesToSketchTupleImpl() {
      targetK_ = DEFAULT_TARGET_K;
    }

    /**
     * VarOpt sampling constructor.
     * @param kStr String indicating the maximum number of desired samples to return.
     */
    public RawTuplesToSketchTupleImpl(final String kStr) {
      targetK_ = Integer.parseInt(kStr);

      if (targetK_ < 1) {
        throw new IllegalArgumentException("VarOpt requires target reservoir size >= 1: "
                + targetK_);
      }
    }

    @Override
    public Tuple exec(final Tuple inputTuple) throws IOException {
      if (inputTuple == null || inputTuple.size() < 1 || inputTuple.isNull(0)) {
        return null;
      }

      final VarOptItemsSketch<Tuple> sketch = rawTuplesToSketch(inputTuple, targetK_);
      return wrapSketchInTuple(sketch);
    }
  }

  public static class UnionSketchesAsTuple extends EvalFunc<Tuple> {
    private final int targetK_;

    public UnionSketchesAsTuple() {
      targetK_ = DEFAULT_TARGET_K;
    }

    /**
     * VarOpt sampling constructor.
     * @param kStr String indicating the maximum number of desired samples to return.
     */
    public UnionSketchesAsTuple(final String kStr) {
      targetK_ = Integer.parseInt(kStr);

      if (targetK_ < 1) {
        throw new IllegalArgumentException("VarOpt requires target sample size >= 1: "
                + targetK_);
      }
    }

    @Override
    public Tuple exec(final Tuple inputTuple) throws IOException {
      if (inputTuple == null || inputTuple.size() < 1 || inputTuple.isNull(0)) {
        return null;
      }

      final VarOptItemsUnion<Tuple> union = unionSketches(inputTuple, targetK_);
      return wrapSketchInTuple(union.getResult());
    }
  }

  public static class UnionSketchesAsByteArray extends EvalFunc<DataByteArray> {
    private final int targetK_;

    public UnionSketchesAsByteArray() {
      targetK_ = DEFAULT_TARGET_K;
    }

    /**
     * VarOpt sampling constructor.
     * @param kStr String indicating the maximum number of desired samples to return.
     */
    public UnionSketchesAsByteArray(final String kStr) {
      targetK_ = Integer.parseInt(kStr);

      if (targetK_ < 1) {
        throw new IllegalArgumentException("VarOpt requires target sample size >= 1: "
                + targetK_);
      }
    }

    @Override
    public DataByteArray exec(final Tuple inputTuple) throws IOException {
      if (inputTuple == null || inputTuple.size() < 1 || inputTuple.isNull(0)) {
        return null;
      }

      final VarOptItemsUnion<Tuple> union = unionSketches(inputTuple, targetK_);
      return new DataByteArray(union.getResult().toByteArray(SERDE));
    }
  }

}
