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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.datasketches.sampling.ReservoirItemsSketch;
import org.apache.datasketches.sampling.ReservoirItemsUnion;
import org.apache.datasketches.sampling.SamplingPigUtil;
import org.apache.pig.AccumulatorEvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * This is a Pig UDF that unions reservoir samples. It implements
 * the <tt>Accumulator</tt> interface for more efficient performance. Input is
 * assumed to come from the reservoir sampling UDF or to be in a compatible format:
 * <tt>(n, k, {(samples)}</tt>
 * where <tt>n</tt> is the total number of items presented to the sketch and <tt>k</tt> is the
 * maximum size of the sketch.
 *
 * @author Jon Malkin
 */
public class ReservoirUnion extends AccumulatorEvalFunc<Tuple> {

  private static final int DEFAULT_TARGET_K = 1024;

  private final int maxK_;
  private ReservoirItemsUnion<Tuple> union_;

  /**
   * Reservoir sampling constructor.
   * @param kStr String indicating the maximum number of desired entries in the reservoir.
   */
  public ReservoirUnion(final String kStr) {
    maxK_ = Integer.parseInt(kStr);

    if (maxK_ < 2) {
      throw new IllegalArgumentException("ReservoirUnion requires max reservoir size >= 2: "
              + maxK_);
    }
  }

  ReservoirUnion() { maxK_ = DEFAULT_TARGET_K; }

  // We could overload exec() for easy cases, but we still need to compare the incoming
  // reservoir's k vs max k and possibly downsample.
  @Override
  public void accumulate(final Tuple inputTuple) throws IOException {
    if (inputTuple == null || inputTuple.size() < 1 || inputTuple.isNull(0)) {
      return;
    }

    final DataBag reservoirs = (DataBag) inputTuple.get(0);

    if (union_ == null) {
      union_ = ReservoirItemsUnion.newInstance(maxK_);
    }

    try {
      for (Tuple t : reservoirs) {
        // if t == null or t.size() < 3, we'll throw an exception
        final long n = (long) t.get(0);
        final int k = (int) t.get(1);
        final DataBag sampleBag = (DataBag) t.get(2);
        final ArrayList<Tuple> samples = ReservoirSampling.dataBagToArrayList(sampleBag);
        union_.update(n, k, samples);
      }
    } catch (final IndexOutOfBoundsException e) {
      throw new ExecException("Cannot update union with given reservoir", e);
    }
  }

  @Override
  public Tuple getValue() {
    if (union_ == null) {
      return null;
    }

    // newDefaultBag(List<Tuple>) does *not* copy values
    final ReservoirItemsSketch<Tuple> resultSketch = union_.getResult();
    final List<Tuple> data = SamplingPigUtil.getRawSamplesAsList(resultSketch);
    final DataBag sampleBag = BagFactory.getInstance().newDefaultBag(data);

    return ReservoirSampling.createResultTuple(resultSketch.getN(), resultSketch.getK(), sampleBag);
  }

  @Override
  public void cleanup() {
    union_ = null;
  }

  /**
   * Validates format of input schema and returns a matching schema
   * @param input Expects input to be a bag of sketches: <tt>(n, k, {(samples...)})</tt>
   * @return Schema based on the
   */
  @Override
  public Schema outputSchema(final Schema input) {
    if (input != null && input.size() > 0) {
      try {
        Schema source = input;

        // if we have a bag, grab one level down to get a tuple
        if (source.size() == 1 && source.getField(0).type == DataType.BAG) {
          source = source.getField(0).schema;
        }

        if (source.size() == 1 && source.getField(0).type == DataType.TUPLE) {
          source = source.getField(0).schema;
        }

        final List<Schema.FieldSchema> fields = source.getFields();
        if (fields.size() == 3
                && fields.get(0).type == DataType.LONG
                && fields.get(1).type == DataType.INTEGER
                && fields.get(2).type == DataType.BAG) {
          return new Schema(new Schema.FieldSchema(getSchemaName(this
                  .getClass().getName().toLowerCase(), source), source, DataType.TUPLE));
        }
      } catch (final FrontendException e) {
        throw new RuntimeException(e);
      }
    }

    return null;
  }
}
