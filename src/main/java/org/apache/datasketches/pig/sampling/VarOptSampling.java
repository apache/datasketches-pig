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

import static org.apache.datasketches.pig.sampling.VarOptCommonImpl.DEFAULT_TARGET_K;
import static org.apache.datasketches.pig.sampling.VarOptCommonImpl.DEFAULT_WEIGHT_IDX;
import static org.apache.datasketches.pig.sampling.VarOptCommonImpl.RECORD_ALIAS;
import static org.apache.datasketches.pig.sampling.VarOptCommonImpl.WEIGHT_ALIAS;
import static org.apache.datasketches.pig.sampling.VarOptCommonImpl.createDataBagFromSketch;
import static org.apache.datasketches.pig.sampling.VarOptCommonImpl.unionSketches;

import java.io.IOException;

import org.apache.datasketches.sampling.VarOptItemsSketch;
import org.apache.datasketches.sampling.VarOptItemsUnion;
import org.apache.pig.AccumulatorEvalFunc;
import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * Applies VarOpt sampling to input tuples. Implements both the <tt>Accumulator</tt> and
 * <tt>Algebraic</tt> interfaces for efficient performance.
 *
 * @author Jon Malkin
 */
@SuppressWarnings("javadoc")
public class VarOptSampling extends AccumulatorEvalFunc<DataBag> implements Algebraic {
  private final int targetK_;
  private final int weightIdx_;
  private VarOptItemsSketch<Tuple> sketch_;

  /**
   * VarOpt sampling constructor.
   * @param kStr String indicating the maximum number of desired samples to return.
   */
  public VarOptSampling(final String kStr) {
    this.targetK_ = Integer.parseInt(kStr);
    this.weightIdx_ = DEFAULT_WEIGHT_IDX;

    if (this.targetK_ < 1) {
      throw new IllegalArgumentException("VarOptSampling requires target sample size >= 1: "
              + this.targetK_);
    }
  }

  /**
   * VarOpt sampling constructor.
   * @param kStr String indicating the maximum number of desired samples to return.
   * @param weightIdxStr String indicating column index (0-based) of weight values
   */
  public VarOptSampling(final String kStr, final String weightIdxStr) {
    this.targetK_ = Integer.parseInt(kStr);
    this.weightIdx_ = Integer.parseInt(weightIdxStr);

    if (this.targetK_ < 1) {
      throw new IllegalArgumentException("VarOptSampling requires target sample size >= 1: "
              + this.targetK_);
    }
    if (this.weightIdx_ < 0) {
      throw new IllegalArgumentException("VarOptSampling requires weight index >= 0: "
              + this.weightIdx_);
    }
  }

  VarOptSampling() {
    this.targetK_ = DEFAULT_TARGET_K;
    this.weightIdx_ = DEFAULT_WEIGHT_IDX;
  }

  @Override
  public void accumulate(final Tuple inputTuple) throws IOException {
    if ((inputTuple == null) || (inputTuple.size() < 1) || inputTuple.isNull(0)) {
      return;
    }

    final DataBag samples = (DataBag) inputTuple.get(0);

    if (this.sketch_ == null) {
      this.sketch_ = VarOptItemsSketch.newInstance(this.targetK_);
    }

    for (final Tuple t : samples) {
      final double weight = (double) t.get(this.weightIdx_);
      this.sketch_.update(t, weight);
    }
  }

  @Override
  public DataBag getValue() {
    if (this.sketch_ == null) {
      return null;
    }

    return createDataBagFromSketch(this.sketch_);
  }

  @Override
  public void cleanup() {
    this.sketch_ = null;
  }

  @Override
  public Schema outputSchema(final Schema input) {
    try {
      if ((input == null) || (input.size() == 0)) {
        throw new IllegalArgumentException("Degenerate input schema to VarOptSampling");
      }

      // first element must be a bag, weightIdx_ element of tuples must be a float or double
      if (input.getField(0).type != DataType.BAG) {
        throw new IllegalArgumentException("VarOpt input must be a data bag: "
                + input.toString());
      }

      final Schema record = input.getField(0).schema; // record has a tuple in field 0
      final Schema fields = record.getField(0).schema;
      if ((fields.getField(this.weightIdx_).type != DataType.DOUBLE)
              && (fields.getField(this.weightIdx_).type != DataType.FLOAT)) {
        throw new IllegalArgumentException("weightIndex item of VarOpt tuple must be a "
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
      throw new RuntimeException(e);
    }
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
    private final int weightIdx_;

    public Final() {
      this.targetK_ = DEFAULT_TARGET_K;
      this.weightIdx_ = DEFAULT_WEIGHT_IDX;
    }

    /**
     * Reducer VarOpt sampling constructor.
     * @param kStr String indicating the maximum number of desired samples to return.
     */
    public Final(final String kStr) {
      this.targetK_ = Integer.parseInt(kStr);
      this.weightIdx_ = DEFAULT_WEIGHT_IDX;

      if (this.targetK_ < 1) {
        throw new IllegalArgumentException("ReservoirSampling requires target reservoir size >= 1: "
                + this.targetK_);
      }
    }

    /**
     * VarOpt sampling constructor.
     * @param kStr String indicating the maximum number of desired samples to return.
     * @param weightIdxStr String indicating column index (0-based) of weight values
     */
    public Final(final String kStr, final String weightIdxStr) {
      this.targetK_ = Integer.parseInt(kStr);
      this.weightIdx_ = Integer.parseInt(weightIdxStr);

      if (this.targetK_ < 1) {
        throw new IllegalArgumentException("VarOptSampling requires target sample size >= 1: "
                + this.targetK_);
      }
      if (this.weightIdx_ < 0) {
        throw new IllegalArgumentException("VarOptSampling requires weight index >= 0: "
                + this.weightIdx_);
      }
    }

    @Override
    public DataBag exec(final Tuple inputTuple) throws IOException {
      if ((inputTuple == null) || (inputTuple.size() < 1) || inputTuple.isNull(0)) {
        return null;
      }

      final VarOptItemsUnion<Tuple> union = unionSketches(inputTuple, this.targetK_);
      return createDataBagFromSketch(union.getResult());
    }
  }
}
