/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.pig.sampling;

import static com.yahoo.sketches.pig.sampling.VarOptCommonImpl.RECORD_ALIAS;
import static com.yahoo.sketches.pig.sampling.VarOptCommonImpl.WEIGHT_ALIAS;
import static com.yahoo.sketches.pig.sampling.VarOptCommonImpl.createDataBagFromSketch;
import static com.yahoo.sketches.pig.sampling.VarOptCommonImpl.unionSketches;

import java.io.IOException;

import org.apache.pig.AccumulatorEvalFunc;
import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.yahoo.sketches.sampling.VarOptItemsSketch;
import com.yahoo.sketches.sampling.VarOptItemsUnion;

/**
 * Applies VarOpt sampling to input tuples. Implements both the <tt>Accumulator</tt> and
 * <tt>Algebraic</tt> interfaces for efficient performance.
 *
 * @author Jon Malkin
 */
public class VarOptSampling extends AccumulatorEvalFunc<DataBag> implements Algebraic {
  private static final int DEFAULT_TARGET_K = 1024;

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

    return createDataBagFromSketch(sketch_);
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
    return VarOptCommonImpl.RawTuplesToSketchTuple.class.getName();
  }

  @Override
  public String getIntermed() {
    return VarOptCommonImpl.UnionSketchesAsTuple.class.getName();
  }

  @Override
  public String getFinal() {
    return Final.class.getName();
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

      final VarOptItemsUnion<Tuple> union = unionSketches(inputTuple, targetK_);
      return createDataBagFromSketch(union.getResult());
    }
  }
}
