/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.pig.sampling;

import java.io.IOException;

import org.apache.pig.AccumulatorEvalFunc;
import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.sampling.VarOptItemsSamples;
import com.yahoo.sketches.sampling.VarOptItemsSketch;
import com.yahoo.sketches.sampling.VarOptItemsUnion;

/**
 * Applies VarOpt sampling to input tuples. Implements both the <tt>Accumulator</tt> and
 * <tt>Algebraic</tt> interfaces for efficient performance.
 *
 * @author Jon Malkin
 */
public class VarOptSampling extends AccumulatorEvalFunc<DataBag> implements Algebraic {
  // defined for test consistency
  static final String WEIGHT_ALIAS = "vo_weight";
  static final String RECORD_ALIAS = "record";

  private static final int DEFAULT_TARGET_K = 1024;

  private static final BagFactory BAG_FACTORY = BagFactory.getInstance();
  private static final TupleFactory TUPLE_FACTORY = TupleFactory.getInstance();
  private static final ArrayOfTuplesSerDe serDe_ = new ArrayOfTuplesSerDe();

  private final int targetK_;
  private VarOptItemsSketch<Tuple> sketch_;

  /**
   * VarOpt sampling constructor.
   * @param kStr String indicating the maximum number of desired samples to return.
   */
  public VarOptSampling(final String kStr) {
    targetK_ = Integer.parseInt(kStr);

    if (targetK_ < 1) {
      throw new IllegalArgumentException("VarOptSampling requires target sample size >= 1: "
              + targetK_);
    }
  }

  VarOptSampling() {
    targetK_ = DEFAULT_TARGET_K;
  }

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
  public DataBag getValue() {
    if (sketch_ == null) {
      return null;
    }

    return createResultFromSketch(sketch_);
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

        final Schema weightedSampleSchema = new Schema();
        weightedSampleSchema.add(new Schema.FieldSchema(WEIGHT_ALIAS, DataType.DOUBLE));
        weightedSampleSchema.add(new Schema.FieldSchema(RECORD_ALIAS, record, DataType.TUPLE));

        return new Schema(new Schema.FieldSchema(getSchemaName(this
                .getClass().getName().toLowerCase(), record), weightedSampleSchema, DataType.BAG));
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
    return Intermediate.class.getName();
  }

  @Override
  public String getFinal() {
    return Final.class.getName();
  }

  public static class Initial extends EvalFunc<Tuple> {
    private final int targetK_;

    public Initial() {
      targetK_ = DEFAULT_TARGET_K;
    }

    /**
     * Map-side VarOpt sampling constructor.
     * @param kStr String indicating the maximum number of desired samples to return.
     */
    public Initial(final String kStr) {
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

      final DataBag records = (DataBag) inputTuple.get(0);

      final VarOptItemsSketch<Tuple> sketch = VarOptItemsSketch.newInstance(targetK_);

      for (Tuple t : records) {
        sketch.update(t, (double) t.get(0));
      }

      final DataByteArray dba = new DataByteArray(sketch.toByteArray(serDe_));
      final Tuple outputTuple = TUPLE_FACTORY.newTuple(1);
      outputTuple.set(0, dba);
      return outputTuple;
    }
  }

  public static class Intermediate extends EvalFunc<Tuple> {
    private final int targetK_;

    public Intermediate() {
      targetK_ = DEFAULT_TARGET_K;
    }

    /**
     * Combiner VarOpt sampling constructor.
     * @param kStr String indicating the maximum number of desired samples to return.
     */
    public Intermediate(final String kStr) {
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
        union.update(mem, serDe_);
      }

      final DataByteArray dba = new DataByteArray(union.getResult().toByteArray(serDe_));
      final Tuple outputTuple = TUPLE_FACTORY.newTuple(1);
      outputTuple.set(0, dba);
      return outputTuple;
    }
  }

  public static class Final extends EvalFunc<DataBag> {
    private final int targetK_;

    public Final() {
      targetK_ = DEFAULT_TARGET_K;
    }

    /**
     * Reducer VarOpt sampling constructor.
     * @param kStr String indicating the maximum number of desired samples to return.
     */
    public Final(final String kStr) {
      targetK_ = Integer.parseInt(kStr);

      if (targetK_ < 1) {
        throw new IllegalArgumentException("ReservoirSampling requires target reservoir size >= 1: "
                + targetK_);
      }
    }

    @Override
    public DataBag exec(final Tuple inputTuple) throws IOException {
      if (inputTuple == null || inputTuple.size() < 1 || inputTuple.isNull(0)) {
        return null;
      }

      final VarOptItemsUnion<Tuple> union = VarOptItemsUnion.newInstance(targetK_);

      final DataBag sketches = (DataBag) inputTuple.get(0);
      for (Tuple t : sketches) {
        final DataByteArray dba = (DataByteArray) t.get(0);
        final Memory mem = Memory.wrap(dba.get());
        union.update(mem, serDe_);
      }

      final VarOptItemsSketch<Tuple> result = union.getResult();

      return VarOptSampling.createResultFromSketch(result);
    }
  }

  private static DataBag createResultFromSketch(final VarOptItemsSketch<Tuple> sketch) {
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
}
