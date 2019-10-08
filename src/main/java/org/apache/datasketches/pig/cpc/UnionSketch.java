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

import static org.apache.datasketches.Util.DEFAULT_UPDATE_SEED;

import java.io.IOException;

import org.apache.datasketches.cpc.CpcSketch;
import org.apache.datasketches.cpc.CpcUnion;
import org.apache.log4j.Logger;
import org.apache.pig.Accumulator;
import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;

/**
 * This is a Pig UDF that performs the Union operation on CpcSketches.
 * This class implements both the <i>Accumulator</i> and <i>Algebraic</i> interfaces.
 *
 * @author Alexander Saydakov
 */
public class UnionSketch extends EvalFunc<DataByteArray> implements Accumulator<DataByteArray>, Algebraic {

  private DataByteArray emptySketch_; // this is to cash an empty sketch

  private final int lgK_;
  private final long seed_;
  private CpcUnion accumUnion_;
  private boolean isFirstCall_;

  /**
   * Constructor with default lgK and target HLL type
   */
  public UnionSketch() {
    this(CpcSketch.DEFAULT_LG_K, DEFAULT_UPDATE_SEED);
  }

  /**
   * Constructor with given lgK as string and default seed.
   *
   * @param lgK in a form of a String
   */
  public UnionSketch(final String lgK) {
    this(Integer.parseInt(lgK), DEFAULT_UPDATE_SEED);
  }

  /**
   * Constructor with given lgK and seed as strings
   *
   * @param lgK in a form of a String
   * @param seed in a form of a String
   */
  public UnionSketch(final String lgK, final String seed) {
    this(Integer.parseInt(lgK), Long.parseLong(seed));
  }

  /**
   * Base constructor.
   *
   * @param lgK parameter controlling the sketch size and accuracy
   * @param seed for the hash function
   */
  public UnionSketch(final int lgK, final long seed) {
    super();
    lgK_ = lgK;
    seed_ = seed;
  }

  /**
   * Top-level exec function.
   * This method accepts an input Tuple containing a Bag of one or more inner <b>Sketch Tuples</b>
   * and returns a single serialized CpcSketch as a DataByteArray.
   *
   * <b>Sketch Tuple</b> is a Tuple containing a single DataByteArray (BYTEARRAY in Pig), which
   * is a serialized HllSketch.
   *
   * @param inputTuple A tuple containing a single bag, containing Sketch Tuples.
   * @return serialized CpcSketch
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
        emptySketch_ = new DataByteArray(new CpcSketch(lgK_, seed_).toByteArray());
      }
      return emptySketch_;
    }
    final CpcUnion union = new CpcUnion(lgK_, seed_);
    final DataBag bag = (DataBag) inputTuple.get(0);
    updateUnion(bag, union, seed_);
    return new DataByteArray(union.getResult().toByteArray());
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
      accumUnion_ = new CpcUnion(lgK_, seed_);
    }
    updateUnion(bag, accumUnion_, seed_);
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
            new CpcSketch(lgK_, seed_).toByteArray());
      }
      return emptySketch_;
    }
    return new DataByteArray(accumUnion_.getResult().toByteArray());
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

  static void updateUnion(final DataBag bag, final CpcUnion union, final long seed) throws ExecException {
    // Bag is not empty. process each innerTuple in the bag
    for (final Tuple innerTuple : bag) {
      final Object f0 = innerTuple.get(0); // consider only field 0
      if (f0 == null) {
        continue;
      }
      final byte type = innerTuple.getType(0);
      if (type == DataType.BYTEARRAY) {
        final DataByteArray dba = (DataByteArray) f0;
        union.update(CpcSketch.heapify(dba.get(), seed));
      } else {
        throw new IllegalArgumentException("Field type was not DataType.BYTEARRAY: " + type);
      }
    }
  }

}
