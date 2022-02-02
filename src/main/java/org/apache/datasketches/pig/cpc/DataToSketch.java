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
 * This is a Pig UDF that builds sketches from data.
 * This class implements both the <i>Accumulator</i> and <i>Algebraic</i> interfaces.
 *
 * @author Alexander Saydakov
 */
public class DataToSketch extends EvalFunc<DataByteArray> implements Accumulator<DataByteArray>, Algebraic {

  private DataByteArray emptySketch_; // this is to cash an empty sketch

  private final int lgK_;
  private final long seed_;
  private CpcSketch accumSketch_;
  private boolean isFirstCall_; // for logging

  /**
   * Constructor with default lgK and seed
   */
  public DataToSketch() {
    this(CpcSketch.DEFAULT_LG_K, DEFAULT_UPDATE_SEED);
  }

  /**
   * Constructor with given lgK as string and default seed
   *
   * @param lgK in a form of a String
   */
  public DataToSketch(final String lgK) {
    this(Integer.parseInt(lgK), DEFAULT_UPDATE_SEED);
  }

  /**
   * Constructor with given lgK and seed as strings
   *
   * @param lgK in a form of a String
   * @param seed in a form of a String
   */
  public DataToSketch(final String lgK, final String seed) {
    this(Integer.parseInt(lgK), Long.parseLong(seed));
  }

  /**
   * Base constructor.
   *
   * @param lgK parameter controlling the sketch size and accuracy
   * @param seed parameter to use during hashing
   */
  public DataToSketch(final int lgK, final long seed) {
    super();
    this.lgK_ = lgK;
    this.seed_ = seed;
  }

  /**
   * Top-level exec function.
   * This method accepts an input Tuple containing a Bag of one or more inner <b>Datum Tuples</b>
   * and returns a single serialized CpcSketch as a DataByteArray.
   *
   * <b>Datum Tuple</b> is a Tuple containing a single field, which can be one of the following
   * (Java type: Pig type):
   * <ul>
   *   <li>Byte: BYTE</li>
   *   <li>Integer: INTEGER</li>
   *   <li>Long: LONG</li>
   *   <li>Float: FLOAT</li>
   *   <li>Double: DOUBLE</li>
   *   <li>String: CHARARRAY</li>
   *   <li>DataByteArray: BYTEARRAY</li>
   * </ul>
   *
   * <p><b>Note</b> Strings as values are normally typed as DataType.CHARARRAY, which will be
   * encoded as UTF-8 prior to being submitted to the sketch. If the user requires a different
   * encoding for cross-platform compatibility, it is recommended that these values be encoded prior
   * to being submitted in a DataBag and then typed as a DataType.BYTEARRAY.</p>
   *
   * @param inputTuple A tuple containing a single bag, containing Datum Tuples.
   * @return serialized CpcSketch
   * @see "org.apache.pig.EvalFunc.exec(org.apache.pig.data.Tuple)"
   * @throws IOException from Pig
   */

  @Override
  public DataByteArray exec(final Tuple inputTuple) throws IOException {
    if (this.isFirstCall_) {
      Logger.getLogger(getClass()).info("Exec was used");
      this.isFirstCall_ = false;
    }
    if (inputTuple == null || inputTuple.size() == 0) {
      if (this.emptySketch_ == null) {
        this.emptySketch_ = new DataByteArray(new CpcSketch(this.lgK_, this.seed_).toByteArray());
      }
      return this.emptySketch_;
    }
    final CpcSketch sketch = new CpcSketch(this.lgK_, this.seed_);
    final DataBag bag = (DataBag) inputTuple.get(0);
    updateSketch(bag, sketch);
    return new DataByteArray(sketch.toByteArray());
  }

  /**
   * An <i>Accumulator</i> version of the standard <i>exec()</i> method. Like <i>exec()</i>,
   * accumulator is called with a bag of Datum Tuples. Unlike <i>exec()</i>, it doesn't serialize the
   * result at the end. Instead, it can be called multiple times, each time with another bag of
   * Datum Tuples to be input to the sketch.
   *
   * @param inputTuple A tuple containing a single bag, containing Datum Tuples.
   * @see #exec
   * @see "org.apache.pig.Accumulator.accumulate(org.apache.pig.data.Tuple)"
   * @throws IOException by Pig
   */
  @Override
  public void accumulate(final Tuple inputTuple) throws IOException {
    if (this.isFirstCall_) {
      Logger.getLogger(getClass()).info("Accumulator was used");
      this.isFirstCall_ = false;
    }
    if (inputTuple == null || inputTuple.size() == 0) { return; }
    final DataBag bag = (DataBag) inputTuple.get(0);
    if (bag == null) { return; }
    if (this.accumSketch_ == null) {
      this.accumSketch_ = new CpcSketch(this.lgK_);
    }
    updateSketch(bag, this.accumSketch_);
  }

  /**
   * Returns the sketch that has been built up by multiple calls to {@link #accumulate}.
   *
   * @return serialized CpcSketch
   * @see "org.apache.pig.Accumulator.getValue()"
   */
  @Override
  public DataByteArray getValue() {
    if (this.accumSketch_ == null) {
      if (this.emptySketch_ == null) {
        this.emptySketch_ = new DataByteArray(new CpcSketch(this.lgK_, this.seed_).toByteArray());
      }
      return this.emptySketch_;
    }
    return new DataByteArray(this.accumSketch_.toByteArray());
  }

  /**
   * Cleans up the UDF state after being called using the {@link Accumulator} interface.
   *
   * @see "org.apache.pig.Accumulator.cleanup()"
   */
  @Override
  public void cleanup() {
    this.accumSketch_ = null;
  }

  @Override
  public String getInitial() {
    return AlgebraicInitial.class.getName();
  }

  @Override
  public String getIntermed() {
    return DataToSketchAlgebraicIntermediate.class.getName();
  }

  @Override
  public String getFinal() {
    return DataToSketchAlgebraicFinal.class.getName();
  }

  static void updateSketch(final DataBag bag, final CpcSketch sketch) throws ExecException {
    // bag is not empty, process each innerTuple in the bag
    for (final Tuple innerTuple: bag) {
      final Object f0 = innerTuple.get(0); // consider only field 0
      if (f0 == null) {
        continue;
      }
      final byte type = innerTuple.getType(0);

      switch (type) {
        case DataType.NULL:
          break;
        case DataType.BYTE:
          sketch.update((byte) f0);
          break;
        case DataType.INTEGER:
          sketch.update((int) f0);
          break;
        case DataType.LONG:
          sketch.update((long) f0);
          break;
        case DataType.FLOAT:
          sketch.update((float) f0);
          break;
        case DataType.DOUBLE:
          sketch.update((double) f0);
          break;
        case DataType.BYTEARRAY: {
          final DataByteArray dba = (DataByteArray) f0;
          sketch.update(dba.get());
          break;
        }
        case DataType.CHARARRAY: {
          final String str = (String) f0;
          // conversion to char[] avoids costly UTF-8 encoding
          sketch.update(str.toCharArray());
          break;
        }
        default:
          throw new IllegalArgumentException("Field 0 of innerTuple must be one of "
              + "NULL, BYTE, INTEGER, LONG, FLOAT, DOUBLE, BYTEARRAY or CHARARRAY. "
              + "Given Type = " + DataType.findTypeName(type)
              + ", Object = " + f0.toString());
      }
    }
  }

}
