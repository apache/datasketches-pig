/*
 * Copyright 2017, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.pig.hll;

import static com.yahoo.sketches.pig.hll.DataToSketch.DEFAULT_HLL_TYPE;
import static com.yahoo.sketches.pig.hll.DataToSketch.DEFAULT_LG_K;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.pig.Accumulator;
import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.hll.HllSketch;
import com.yahoo.sketches.hll.TgtHllType;
import com.yahoo.sketches.hll.Union;

/**
 * This is a Pig UDF that performs the Union operation on HllSketches.
 * This class implements both the <i>Accumulator</i> and <i>Algebraic</i> interfaces.
 *
 * @author Alexander Saydakov
 */
public class UnionSketch extends EvalFunc<DataByteArray> implements Accumulator<DataByteArray>, Algebraic {

  private DataByteArray emptySketch_; // this is to cash an empty sketch

  private final int lgK_;
  private final TgtHllType tgtHllType_;
  private Union accumUnion_;
  private boolean isFirstCall_;

  /**
   * Constructor with default lgK and target HLL type
   */
  public UnionSketch() {
    this(DEFAULT_LG_K, DEFAULT_HLL_TYPE);
  }

  /**
   * Constructor with given lgK as string and default target HLL type.
   *
   * @param lgK in a form of a String
   */
  public UnionSketch(final String lgK) {
    this(Integer.parseInt(lgK), DEFAULT_HLL_TYPE);
  }

  /**
   * Constructor with given lgK and target HLL type as strings
   *
   * @param lgK in a form of a String
   * @param tgtHllType in a form of a String
   */
  public UnionSketch(final String lgK, final String tgtHllType) {
    this(Integer.parseInt(lgK), TgtHllType.valueOf(tgtHllType));
  }

  /**
   * Base constructor.
   *
   * @param lgK parameter controlling the sketch size and accuracy
   * @param tgtHllType HLL type of the resulting sketch
   */
  public UnionSketch(final int lgK, final TgtHllType tgtHllType) {
    super();
    lgK_ = lgK;
    tgtHllType_ = tgtHllType;
  }

  /**
   * Top-level exec function.
   * This method accepts an input Tuple containing a Bag of one or more inner <b>Sketch Tuples</b>
   * and returns a single serialized HllSketch as a DataByteArray.
   *
   * <b>Sketch Tuple</b> is a Tuple containing a single DataByteArray (BYTEARRAY in Pig), which
   * is a serialized HllSketch.
   *
   * @param inputTuple A tuple containing a single bag, containing Sketch Tuples.
   * @return serialized HllSketch
   * @see "org.apache.pig.EvalFunc.exec(org.apache.pig.data.Tuple)"
   * @throws IOException from Pig.
   */
  @Override
  public DataByteArray exec(final Tuple inputTuple) throws IOException {
    if (isFirstCall_) {
      Logger.getLogger(getClass()).info("Exec was used");
      isFirstCall_ = false;
    }
    if (inputTuple == null || inputTuple.size() == 0) {
      if (emptySketch_ == null) {
        emptySketch_ = new DataByteArray(new HllSketch(lgK_, tgtHllType_).toCompactByteArray());
      }
      return emptySketch_;
    }
    final Union union = new Union(lgK_);
    final DataBag bag = (DataBag) inputTuple.get(0);
    updateUnion(bag, union);
    return new DataByteArray(union.getResult(tgtHllType_).toCompactByteArray());
  }

  /**
   * An <i>Accumulator</i> version of the standard <i>exec()</i> method. Like <i>exec()</i>,
   * accumulator is called with a bag of Sketch Tuples. Unlike <i>exec()</i>, it doesn't serialize the
   * result at the end. Instead, it can be called multiple times, each time with another bag of
   * Sketch Tuples to be input to the union.
   *
   * @param inputTuple A tuple containing a single bag, containing Sketch Tuples.
   * @see #exec
   * @see "org.apache.pig.Accumulator.accumulate(org.apache.pig.data.Tuple)"
   * @throws IOException by Pig
   */
  @Override
  public void accumulate(final Tuple inputTuple) throws IOException {
    if (isFirstCall_) {
      Logger.getLogger(getClass()).info("Accumulator was used");
      isFirstCall_ = false;
    }
    if (inputTuple == null || inputTuple.size() == 0) { return; }
    final DataBag bag = (DataBag) inputTuple.get(0);
    if (bag == null) { return; }
    if (accumUnion_ == null) {
      accumUnion_ = new Union(lgK_);
    }
    updateUnion(bag, accumUnion_);
  }

  /**
   * Returns the sketch that has been built up by multiple calls to {@link #accumulate}.
   *
   * @return Sketch Tuple. (see {@link #exec} for return tuple format)
   * @see "org.apache.pig.Accumulator.getValue()"
   */
  @Override
  public DataByteArray getValue() {
    if (accumUnion_ == null) {
      if (emptySketch_ == null) {
        emptySketch_ = new DataByteArray(
            new HllSketch(lgK_, tgtHllType_).toCompactByteArray());
      }
      return emptySketch_;
    }
    return new DataByteArray(
        accumUnion_.getResult(tgtHllType_).toCompactByteArray());
  }

  /**
   * Cleans up the UDF state after being called using the {@link Accumulator} interface.
   *
   * @see "org.apache.pig.Accumulator.cleanup()"
   */
  @Override
  public void cleanup() {
    accumUnion_ = null;
  }

  @Override
  public String getInitial() {
    return AlgebraicInitial.class.getName();
  }

  @Override
  public String getIntermed() {
    return UnionSketchAlgebraicIntermediate.class.getName();
  }

  @Override
  public String getFinal() {
    return UnionSketchAlgebraicFinal.class.getName();
  }

  static void updateUnion(final DataBag bag, final Union union) throws ExecException {
    // Bag is not empty. process each innerTuple in the bag
    for (final Tuple innerTuple : bag) {
      final Object f0 = innerTuple.get(0); // consider only field 0
      if (f0 == null) {
        continue;
      }
      final byte type = innerTuple.getType(0);
      if (type == DataType.BYTEARRAY) {
        final DataByteArray dba = (DataByteArray) f0;
        union.update(HllSketch.heapify(Memory.wrap(dba.get())));
      } else {
        throw new IllegalArgumentException("Field type was not DataType.BYTEARRAY: " + type);
      }
    }
  }

}
