/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.pig.sampling;

import java.io.IOException;

import org.apache.pig.AccumulatorEvalFunc;
import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.sampling.VarOptItemsSketch;
import com.yahoo.sketches.sampling.VarOptItemsUnion;

/**
 * This UDF creates a binary version of a VarOpt sampling over input tuples. The resulting
 * <tt>DataByteArray</tt> can be read in Pig with <tt>GetVarOptSamples</tt>, although the
 * per-record schema will be lost. It implements both the <tt>Accumulator</tt> and
 * <tt>Algebraic</tt> interfaces for efficient performance.
 *
 * @author Jon Malkin
 */
public class DataToVarOptSketch extends AccumulatorEvalFunc<DataByteArray> implements Algebraic {
  private static final int DEFAULT_TARGET_K = 1024;
  private static final TupleFactory TUPLE_FACTORY = TupleFactory.getInstance();

  private final int targetK_;
  private VarOptItemsSketch<Tuple> sketch_;
  private ArrayOfTuplesSerDe serDe_ = new ArrayOfTuplesSerDe();

  /**
   * VarOpt sampling constructor.
   * @param kStr String indicating the maximum number of desired entries in the reservoir.
   */
  public DataToVarOptSketch(final String kStr) {
    targetK_ = Integer.parseInt(kStr);

    if (targetK_ < 1) {
      throw new IllegalArgumentException("VarOptSampling requires target sample size >= 1: "
              + targetK_);
    }
  }

  DataToVarOptSketch() { targetK_ = DEFAULT_TARGET_K; }

  @Override
  public void accumulate(final Tuple inputTuple) throws IOException {
    if (inputTuple == null || inputTuple.size() < 1 || inputTuple.isNull(0)) {
      return;
    }

    final DataBag samples = (DataBag) inputTuple.get(0);

    if (sketch_ == null) {
      sketch_ = VarOptItemsSketch.newInstance(targetK_);
    }

    for (Tuple t : samples) {
      // first element is weight
      final double weight = (double) t.get(0);
      sketch_.update(t, weight);
    }
  }

  @Override
  public DataByteArray getValue() {
    if (sketch_ == null) {
      return null;
    }

    return new DataByteArray(sketch_.toByteArray(serDe_));
  }

  @Override
  public void cleanup() {
    sketch_ = null;
  }

  @Override
  public Schema outputSchema(final Schema input) {
    if (input != null && input.size() > 0) {
      try {
        Schema record = input;

        // first element must be a bag, first element of tuples must be the weight (float or double)
        if (record.getField(0).type != DataType.BAG) {
          throw new IllegalArgumentException("VarOpt input must be a data bag: "
                  + record.toString());
        }

        record = record.getField(0).schema; // record has a tuple in field 0
        final Schema fields = record.getField(0).schema; //
        if (fields.getField(0).type != DataType.DOUBLE
                && fields.getField(0).type != DataType.FLOAT) {
          throw new IllegalArgumentException("First item of VarOpt tuple must be a "
                  + "weight (double/float), found " + fields.getField(0).type
                  + ": " + fields.toString());

        }

        return new Schema(new Schema.FieldSchema(getSchemaName(this
                .getClass().getName().toLowerCase(), input), DataType.BYTEARRAY));
      }
      catch (final FrontendException e) {
        // fall through
      }
    }
    return null;
  }


  @Override
  public String getInitial() {
    return Initial.class.getName();
  }

  @Override
  public String getIntermed() {
    return IntermediateFinal.class.getName();
  }

  @Override
  public String getFinal() {
    return IntermediateFinal.class.getName();
  }

  public static class Initial extends EvalFunc<Tuple> {
    private final int targetK_;
    private VarOptItemsSketch<Tuple> sketch_;
    private static ArrayOfTuplesSerDe serDe_ = new ArrayOfTuplesSerDe();

    public Initial() {
      targetK_ = DEFAULT_TARGET_K;
    }

    /** Map-side constructor for VarOpt dataToSketch
     * @param kStr String indicating the maximum number of desired entries in the sample.
     */
    public Initial(final String kStr) {
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

      final DataBag samples = (DataBag) inputTuple.get(0);

      if (sketch_ == null) {
        sketch_ = VarOptItemsSketch.newInstance(targetK_);
      }

      for (Tuple t : samples) {
        // first element is weight
        final double weight = (double) t.get(0);
        sketch_.update(t, weight);
      }

      final Tuple output = TUPLE_FACTORY.newTuple(1);
      output.set(0, new DataByteArray(sketch_.toByteArray(serDe_)));
      return output;
    }
  }

  public static class IntermediateFinal extends EvalFunc<DataByteArray> {
    private final int targetK_;
    private VarOptItemsUnion<Tuple> union_;
    private static ArrayOfTuplesSerDe serDe_ = new ArrayOfTuplesSerDe();

    public IntermediateFinal() {
      targetK_ = DEFAULT_TARGET_K;
    }

    /** Combiner and reducer constructor for VarOpt dataToSketch
     * @param kStr String indicating the maximum number of desired entries in the sample.
     */
    public IntermediateFinal(final String kStr) {
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

      if (union_ == null) {
        union_ = VarOptItemsUnion.newInstance(targetK_);
      }

      Memory mem;
      final DataBag outerBag = (DataBag) inputTuple.get(0);
      for (Tuple reservoir : outerBag) {
        final DataByteArray dba = (DataByteArray) reservoir.get(0);
        mem = Memory.wrap(dba.get());
        union_.update(mem, serDe_);
      }

      final VarOptItemsSketch<Tuple> sketch = union_.getResult();

      return new DataByteArray(sketch.toByteArray(serDe_));
    }
  }
}
