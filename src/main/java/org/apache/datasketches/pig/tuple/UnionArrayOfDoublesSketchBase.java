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

package org.apache.datasketches.pig.tuple;

import static org.apache.datasketches.Util.DEFAULT_NOMINAL_ENTRIES;

import java.io.IOException;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.tuple.ArrayOfDoublesSetOperationBuilder;
import org.apache.datasketches.tuple.ArrayOfDoublesSketches;
import org.apache.datasketches.tuple.ArrayOfDoublesUnion;
import org.apache.datasketches.tuple.ArrayOfDoublesUpdatableSketchBuilder;
import org.apache.log4j.Logger;
import org.apache.pig.Accumulator;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

abstract class UnionArrayOfDoublesSketchBase extends EvalFunc<Tuple> implements Accumulator<Tuple> {
  private final int sketchSize_;
  private final int numValues_;
  private ArrayOfDoublesUnion accumUnion_;
  private boolean isFirstCall_ = true;

  UnionArrayOfDoublesSketchBase() {
    this(DEFAULT_NOMINAL_ENTRIES, 1);
  }

  UnionArrayOfDoublesSketchBase(final int numValues) {
    this(DEFAULT_NOMINAL_ENTRIES, numValues);
  }

  UnionArrayOfDoublesSketchBase(final int sketchSize, final int numValues) {
    super();
    sketchSize_ = sketchSize;
    numValues_ = numValues;
  }

  @Override
  public Tuple exec(final Tuple inputTuple) throws IOException {
    if (isFirstCall_) {
      // this is to see in the log which way was used by Pig
      Logger.getLogger(getClass()).info("exec is used");
      isFirstCall_ = false;
    }
    if ((inputTuple == null) || (inputTuple.size() == 0)) {
      return null;
    }
    final DataBag bag = (DataBag) inputTuple.get(0);
    final ArrayOfDoublesUnion union =
        new ArrayOfDoublesSetOperationBuilder().setNominalEntries(sketchSize_)
          .setNumberOfValues(numValues_).buildUnion();
    updateUnion(bag, union);
    return Util.tupleFactory.newTuple(new DataByteArray(union.getResult().toByteArray()));
  }

  @Override
  public void accumulate(final Tuple inputTuple) throws IOException {
    if (isFirstCall_) {
      // this is to see in the log which way was used by Pig
      Logger.getLogger(getClass()).info("accumulator is used");
      isFirstCall_ = false;
    }
    if ((inputTuple == null) || (inputTuple.size() != 1)) {
      return;
    }
    final DataBag bag = (DataBag) inputTuple.get(0);
    if (bag == null || bag.size() == 0) { return; }
    if (accumUnion_ == null) {
      accumUnion_ = new ArrayOfDoublesSetOperationBuilder().setNominalEntries(sketchSize_)
          .setNumberOfValues(numValues_).buildUnion();
    }
    updateUnion(bag, accumUnion_);
  }

  @Override
  public Tuple getValue() {
    if (accumUnion_ == null) { //return an empty sketch
      return Util.tupleFactory.newTuple(new DataByteArray(
        new ArrayOfDoublesUpdatableSketchBuilder().setNumberOfValues(numValues_).build().compact()
          .toByteArray())
      );
    }
    return Util.tupleFactory.newTuple(new DataByteArray(accumUnion_.getResult().toByteArray()));
  }

  @Override
  public void cleanup() {
    if (accumUnion_ != null) { accumUnion_.reset(); }
  }

  private static void updateUnion(final DataBag bag, final ArrayOfDoublesUnion union)
      throws ExecException {
    for (final Tuple innerTuple: bag) {
      if ((innerTuple.size() != 1) || (innerTuple.get(0) == null)) {
        continue;
      }
      final DataByteArray dba = (DataByteArray) innerTuple.get(0);
      union.update(ArrayOfDoublesSketches.wrapSketch(Memory.wrap(dba.get())));
    }
  }

}
