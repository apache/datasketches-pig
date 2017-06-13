package com.yahoo.sketches.pig.sampling;

import java.io.IOException;

import org.apache.pig.AccumulatorEvalFunc;
import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.sampling.VarOptItemsUnion;

/**
 * Accepts binary VarOpt sketch images and unions them into a single binary output sketch.
 * Due to using opaque binary objects, schema information is unavailable.
 *
 * <p>The VarOpt sketch can handle input sketches with different values of <tt>k</tt>, and will
 * produce a result using the largest number of samples that still produces a valid VarOpt
 * sketch.</p>
 *
 * @author Jon Malkin
 */
public class VarOptUnion extends AccumulatorEvalFunc<DataByteArray> implements Algebraic {
  private static final int DEFAULT_TARGET_K = 1024;
  private static final TupleFactory TUPLE_FACTORY = TupleFactory.getInstance();
  private static final ArrayOfTuplesSerDe SERDE = new ArrayOfTuplesSerDe();

  private final int maxK_;
  private VarOptItemsUnion<Tuple> union_;

  /**
   * VarOpt union constructor.
   * @param kStr String indicating the maximum number of desired entries in the sample.
   */
  public VarOptUnion(final String kStr) {
    maxK_ = Integer.parseInt(kStr);

    if (maxK_ < 2) {
      throw new IllegalArgumentException("ReservoirUnion requires max reservoir size >= 2: "
              + maxK_);
    }
  }

  VarOptUnion() { maxK_ = DEFAULT_TARGET_K; }

  // We could overload exec() for easy cases, but we still need to compare the incoming
  // reservoir's k vs max k and possibly downsample.
  @Override
  public void accumulate(final Tuple inputTuple) throws IOException {
    if (inputTuple == null || inputTuple.size() < 1 || inputTuple.isNull(0)) {
      return;
    }

    final DataBag sketches = (DataBag) inputTuple.get(0);

    if (union_ == null) {
      union_ = VarOptItemsUnion.newInstance(maxK_);
    }

    try {
      for (Tuple t : sketches) {
        final DataByteArray dba = (DataByteArray) t.get(0);
        final Memory sketch = Memory.wrap(dba.get());
        union_.update(sketch, SERDE);
      }
    } catch (final IndexOutOfBoundsException e) {
      throw new ExecException("Cannot update union with given sketch", e);
    }
  }

  @Override
  public DataByteArray getValue() {
    if (union_ == null) {
      return null;
    }

    return new DataByteArray(union_.getResult().toByteArray(SERDE));
  }

  @Override
  public void cleanup() {
    union_ = null;
  }

  @Override
  public Schema outputSchema(final Schema input) {
    return new Schema(new Schema.FieldSchema(getSchemaName(this
            .getClass().getName().toLowerCase(), input), DataType.BYTEARRAY));
  }

  @Override
  public String getInitial() {
    return InitialIntermediate.class.getName();
  }

  @Override
  public String getIntermed() {
    return InitialIntermediate.class.getName();
  }

  @Override
  public String getFinal() {
    return Final.class.getName();
  }

  public static class InitialIntermediate extends EvalFunc<Tuple> {
    private final int targetK_;

    public InitialIntermediate() {
      targetK_ = DEFAULT_TARGET_K;
    }

    /**
     * Mapper and Combiner VarOpt sampling constructor.
     * @param kStr String indicating the maximum number of desired samples to return.
     */
    public InitialIntermediate(final String kStr) {
      targetK_ = Integer.parseInt(kStr);

      if (targetK_ < 1) {
        throw new IllegalArgumentException("ReservoirSampling requires target reservoir size >= 1: "
                + targetK_);
      }
    }

    @Override
    public Tuple exec(final Tuple inputTuple) throws IOException {
      if (inputTuple == null || inputTuple.size() < 1 || inputTuple.isNull(0)) {
        return null;
      }

      final VarOptItemsUnion<Tuple> union = VarOptItemsUnion.newInstance(targetK_);

      final DataBag sketchBag = (DataBag) inputTuple.get(0);
      for (Tuple t : sketchBag) {
        final DataByteArray dba = (DataByteArray) t.get(0);
        final Memory mem = Memory.wrap(dba.get());
        union.update(mem, SERDE);
      }

      final DataByteArray dba = new DataByteArray(union.getResult().toByteArray(SERDE));
      final Tuple outputTuple = TUPLE_FACTORY.newTuple(1);
      outputTuple.set(0, dba);
      return outputTuple;
    }
  }

  public static class Final extends EvalFunc<DataByteArray> {
    private final int targetK_;

    public Final() {
      targetK_ = DEFAULT_TARGET_K;
    }

    /** Reducer constructor for VarOpt dataToSketch
     * @param kStr String indicating the maximum number of desired entries in the sample.
     */
    public Final(final String kStr) {
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

      final DataBag outerBag = (DataBag) inputTuple.get(0);
      final VarOptItemsUnion<Tuple> union_ = VarOptItemsUnion.newInstance(targetK_);

      Memory mem;
      for (Tuple reservoir : outerBag) {
        final DataByteArray dba = (DataByteArray) reservoir.get(0);
        mem = Memory.wrap(dba.get());
        union_.update(mem, SERDE);
      }

      return new DataByteArray(union_.getResult().toByteArray(SERDE));
    }
  }

}
