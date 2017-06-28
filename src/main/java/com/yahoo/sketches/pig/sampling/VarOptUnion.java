/*
 * Copyright 2017, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.pig.sampling;

import static com.yahoo.sketches.pig.sampling.VarOptCommonImpl.DEFAULT_TARGET_K;

import java.io.IOException;

import org.apache.pig.AccumulatorEvalFunc;
import org.apache.pig.Algebraic;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
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
  private static final ArrayOfTuplesSerDe SERDE = new ArrayOfTuplesSerDe();

  private final int maxK_;
  private VarOptItemsUnion<Tuple> union_;

  /**
   * VarOpt union constructor.
   * @param kStr String indicating the maximum number of desired entries in the sample.
   */
  public VarOptUnion(final String kStr) {
    maxK_ = Integer.parseInt(kStr);

    if (maxK_ < 1) {
      throw new IllegalArgumentException("VarOptUnion requires max sample size >= 1: "
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

    for (Tuple t : sketches) {
      final DataByteArray dba = (DataByteArray) t.get(0);
      final Memory sketch = Memory.wrap(dba.get());
      union_.update(sketch, SERDE);
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
  public String getInitial() {
    return VarOptCommonImpl.UnionSketchesAsTuple.class.getName();
  }

  @Override
  public String getIntermed() {
    return VarOptCommonImpl.UnionSketchesAsTuple.class.getName();
  }

  @Override
  public String getFinal() {
    return VarOptCommonImpl.UnionSketchesAsByteArray.class.getName();
  }

  @Override
  public Schema outputSchema(final Schema input) {
    return new Schema(new Schema.FieldSchema(getSchemaName(this
            .getClass().getName().toLowerCase(), input), DataType.BYTEARRAY));
  }
}
