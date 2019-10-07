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

package org.apache.datasketches.pig.cpc;

import java.io.IOException;

import org.apache.datasketches.cpc.CpcSketch;
import org.apache.datasketches.cpc.CpcUnion;
import org.apache.log4j.Logger;
import org.apache.pig.EvalFunc;
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
  private final long seed_;
  private DataByteArray emptySketch_; // this is to cash an empty sketch tuple
  private boolean isFirstCall_ = true; // for logging

  /**
   * Constructor with primitives for the final passes of an Algebraic function.
   *
   * @param lgK parameter controlling the sketch size and accuracy
   * @param seed for the hash function
   */
  public AlgebraicFinal(final int lgK, final long seed) {
    lgK_ = lgK;
    seed_ = seed;
  }

  @Override
  public DataByteArray exec(final Tuple inputTuple) throws IOException {
    if (isFirstCall_) {
      Logger.getLogger(getClass()).info("Algebraic was used");
      isFirstCall_ = false;
    }
    final DataByteArray dba = process(inputTuple, lgK_, seed_, isInputRaw());
    if (dba == null) {
      return getEmptySketch();
    }
    return dba;
  }

  static DataByteArray process(final Tuple inputTuple, final int lgK, final long seed,
      final boolean isInputRaw) throws IOException {
    if ((inputTuple == null) || (inputTuple.size() == 0)) {
      return null;
    }
    CpcSketch sketch = null;
    CpcUnion union = null;
    final DataBag outerBag = (DataBag) inputTuple.get(0);
    if (outerBag == null) {
      return null;
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
        if (isInputRaw) {
          if (sketch == null) {
            sketch = new CpcSketch(lgK, seed);
          }
          DataToSketch.updateSketch(innerBag, sketch);
        } else {
          if (union == null) {
            union = new CpcUnion(lgK, seed);
          }
          UnionSketch.updateUnion(innerBag, union, seed);
        }
      } else if (f0 instanceof DataByteArray) { // inputTuple.bag0.dataTupleN.f0:DBA
        // If field 0 of a dataTuple is a DataByteArray, we assume it is a sketch
        // due to system bagged outputs from multiple mapper Intermediate functions.
        // Each dataTuple.DBA:sketch will merged into the union.
        final DataByteArray dba = (DataByteArray) f0;
        if (union == null) {
          union = new CpcUnion(lgK, seed);
        }
        union.update(CpcSketch.heapify(dba.get(), seed));
      } else { // we should never get here
        throw new IllegalArgumentException("dataTuple.Field0 is not a DataBag or DataByteArray: "
            + f0.getClass().getName());
      }
    }
    if ((sketch != null) && (union != null)) {
      union.update(sketch);
      sketch = null;
    }
    if (sketch != null) {
      return new DataByteArray(sketch.toByteArray());
    } else if (union != null) {
      return new DataByteArray(union.getResult().toByteArray());
    }
    return null;
  }

  abstract boolean isInputRaw();

  private DataByteArray getEmptySketch() {
    if (emptySketch_ == null) {
      emptySketch_ = new DataByteArray(new CpcSketch(lgK_, seed_).toByteArray());
    }
    return emptySketch_;
  }

}
