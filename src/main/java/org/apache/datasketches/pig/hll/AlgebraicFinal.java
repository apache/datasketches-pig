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

package org.apache.datasketches.pig.hll;

import java.io.IOException;

import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.TgtHllType;
import org.apache.datasketches.hll.Union;
import org.apache.datasketches.memory.Memory;
import org.apache.log4j.Logger;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

/**
 * Class used to calculate the final pass of an <i>Algebraic</i> sketch
 * operation. It will receive a bag of values returned by either the <i>Intermediate</i>
 * stage or the <i>Initial</i> stages, so it needs to be able to differentiate between and
 * interpret both types.
 * 
 * @author Alexander Saydakov
 */
abstract class AlgebraicFinal extends EvalFunc<DataByteArray> {

  private final int lgK_;
  private final TgtHllType tgtHllType_;
  private DataByteArray emptySketch_; // this is to cash an empty sketch tuple
  private boolean isFirstCall_ = true; // for logging

  /**
   * Constructor with primitives for the final passes of an Algebraic function.
   *
   * @param lgK parameter controlling the sketch size and accuracy
   * @param tgtHllType HLL type of the resulting sketch
   */
  public AlgebraicFinal(final int lgK, final TgtHllType tgtHllType) {
    lgK_ = lgK;
    tgtHllType_ = tgtHllType;
  }

  @Override
  public DataByteArray exec(final Tuple inputTuple) throws IOException {
    if (isFirstCall_) {
      Logger.getLogger(getClass()).info("Algebraic was used");
      isFirstCall_ = false;
    }
    if (inputTuple == null || inputTuple.size() == 0) {
      return getEmptySketch();
    }
    final Union union = new Union(lgK_);
    final DataBag outerBag = (DataBag) inputTuple.get(0);
    if (outerBag == null) {
      return getEmptySketch();
    }
    for (final Tuple dataTuple: outerBag) {
      final Object f0 = dataTuple.get(0); // inputTuple.bag0.dataTupleN.f0
      if (f0 == null) {
        continue;
      }
      if (f0 instanceof DataBag) {
        final DataBag innerBag = (DataBag) f0; // inputTuple.bag0.dataTupleN.f0:bag
        if (innerBag.size() == 0) { continue; }
        // If field 0 of a dataTuple is a Bag, all innerTuples of this inner bag
        // will be passed into the union.
        // It is due to system bagged outputs from multiple mapper Initial functions.
        // The Intermediate stage was bypassed.
        updateUnion(innerBag, union);
      } else if (f0 instanceof DataByteArray) { // inputTuple.bag0.dataTupleN.f0:DBA
        // If field 0 of a dataTuple is a DataByteArray, we assume it is a sketch
        // due to system bagged outputs from multiple mapper Intermediate functions.
        // Each dataTuple.DBA:sketch will merged into the union.
        final DataByteArray dba = (DataByteArray) f0;
        union.update(HllSketch.wrap(Memory.wrap(dba.get())));
      } else { // we should never get here
        throw new IllegalArgumentException("dataTuple.Field0 is not a DataBag or DataByteArray: "
            + f0.getClass().getName());
      }
    }
    return new DataByteArray(union.getResult(tgtHllType_).toCompactByteArray());
  }

  abstract void updateUnion(DataBag bag, Union union) throws ExecException;

  private DataByteArray getEmptySketch() {
    if (emptySketch_ == null) {
      emptySketch_ = new DataByteArray(
          new HllSketch(lgK_, tgtHllType_).toCompactByteArray());
    }
    return emptySketch_;
  }

}
