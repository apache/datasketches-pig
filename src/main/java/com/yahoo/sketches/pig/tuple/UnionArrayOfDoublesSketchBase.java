/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.pig.tuple;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.pig.Accumulator;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

import com.yahoo.sketches.memory.NativeMemory;
import com.yahoo.sketches.tuple.ArrayOfDoublesSetOperationBuilder;
import com.yahoo.sketches.tuple.ArrayOfDoublesSketches;
import com.yahoo.sketches.tuple.ArrayOfDoublesUnion;
import com.yahoo.sketches.tuple.ArrayOfDoublesUpdatableSketchBuilder;

abstract class UnionArrayOfDoublesSketchBase extends EvalFunc<Tuple> implements Accumulator<Tuple> {
  private final int sketchSize_;
  private final int numValues_;
  private ArrayOfDoublesUnion accumUnion_;
  private boolean isFirstCall_ = true;

  UnionArrayOfDoublesSketchBase(final int sketchSize, final int numValues) {
    super();
    sketchSize_ = sketchSize;
    numValues_ = numValues;
  }

  @Override
  public Tuple exec(Tuple inputTuple) throws IOException {
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
    if (bag == null || bag.size() == 0) return;
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
    if (accumUnion_ != null) accumUnion_.reset();
  }

  private static void updateUnion(final DataBag bag, final ArrayOfDoublesUnion union) 
      throws ExecException {
    for (final Tuple innerTuple: bag) {
      if ((innerTuple.size() != 1) || (innerTuple.get(0) == null)) {
        continue;
      }
      final DataByteArray dba = (DataByteArray) innerTuple.get(0);
      union.update(ArrayOfDoublesSketches.wrapSketch(new NativeMemory(dba.get())));
    }
  }

}
