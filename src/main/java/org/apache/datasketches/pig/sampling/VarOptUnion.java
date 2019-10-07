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

import java.io.IOException;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.sampling.VarOptItemsUnion;
import org.apache.pig.AccumulatorEvalFunc;
import org.apache.pig.Algebraic;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

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
    return union_ == null ? null : new DataByteArray(union_.getResult().toByteArray(SERDE));
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
