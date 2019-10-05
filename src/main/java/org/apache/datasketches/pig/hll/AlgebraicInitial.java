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

import org.apache.log4j.Logger;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

/**
 * Class used to calculate the initial pass of an Algebraic sketch operation.
 *
 * <p>The Initial class simply passes through all records unchanged so that they can be
 * processed by the intermediate processor instead.
 * 
 * @author Alexander Saydakov
 */
public class AlgebraicInitial extends EvalFunc<Tuple> {

  private boolean isFirstCall_ = true;

  /**
   * Default constructor for the initial pass of an Algebraic function.
   */
  public AlgebraicInitial() {}

  /**
   * Constructor for the initial pass of an Algebraic function. Pig will call this and pass the
   * same constructor arguments as the original UDF. In this case the arguments are ignored.
   *
   * @param lgK in a form of a String
   */
  public AlgebraicInitial(final String lgK) {}

  /**
   * Constructor for the initial pass of an Algebraic function. Pig will call this and pass the
   * same constructor arguments as the original UDF. In this case the arguments are ignored.
   *
   * @param lgK in a form of a String
   * @param tgtHllType in a form of a String
   */
  public AlgebraicInitial(final String lgK, final String tgtHllType) {}

  @Override
  public Tuple exec(final Tuple inputTuple) throws IOException {
    if (isFirstCall_) {
      Logger.getLogger(getClass()).info("Algebraic was used");
      isFirstCall_ = false;
    }
    return inputTuple;
  }

}
