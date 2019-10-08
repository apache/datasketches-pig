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
import org.apache.log4j.Logger;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

/**
 * Class used to calculate the intermediate combiner pass of an <i>Algebraic</i> sketch
 * operation. This is called from the combiner, and may be called multiple times (from a mapper
 * and from a reducer). It will receive a bag of values returned by either <i>Intermediate</i>
 * or <i>Initial</i> stages, so it needs to be able to differentiate between and
 * interpret both types.
 *
 * @author Alexander Saydakov
 */
abstract class AlgebraicIntermediate extends EvalFunc<Tuple> {

  private static final TupleFactory TUPLE_FACTORY = TupleFactory.getInstance();

  private final int lgK_;
  private final long seed_;
  private Tuple emptySketchTuple_; // this is to cash an empty sketch tuple
  private boolean isFirstCall_ = true; // for logging

  /**
   * Constructor with primitives for the intermediate pass of an Algebraic function.
   *
   * @param lgK parameter controlling the sketch size and accuracy
   * @param seed the given seed
   */
  public AlgebraicIntermediate(final int lgK, final long seed) {
    lgK_ = lgK;
    seed_ = seed;
  }

  @Override
  public Tuple exec(final Tuple inputTuple) throws IOException {
    if (isFirstCall_) {
      Logger.getLogger(getClass()).info("Algebraic was used");
      isFirstCall_ = false;
    }
    final DataByteArray dba = AlgebraicFinal.process(inputTuple, lgK_, seed_, isInputRaw());
    if (dba == null) {
      return getEmptySketchTuple();
    }
    return TUPLE_FACTORY.newTuple(dba);
  }

  abstract boolean isInputRaw();

  private Tuple getEmptySketchTuple() {
    if (emptySketchTuple_ == null) {
      emptySketchTuple_ = TUPLE_FACTORY.newTuple(new DataByteArray(
          new CpcSketch(lgK_, seed_).toByteArray()));
    }
    return emptySketchTuple_;
  }

}
