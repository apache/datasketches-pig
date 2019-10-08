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

import org.apache.datasketches.cpc.CpcSketch;

@SuppressWarnings("javadoc")
public class DataToSketchAlgebraicIntermediate extends AlgebraicIntermediate {

  /**
   * Default constructor for the intermediate pass of an Algebraic function.
   * Assumes default lgK and seed.
   */
  public DataToSketchAlgebraicIntermediate() {
    super(CpcSketch.DEFAULT_LG_K, DEFAULT_UPDATE_SEED);
  }

  /**
   * Constructor for the intermediate pass of an Algebraic function. Pig will call
   * this and pass the same constructor arguments as the base UDF.
   * Assumes default seed.
   *
   * @param lgK in a form of a String
   */
  public DataToSketchAlgebraicIntermediate(final String lgK) {
    super(Integer.parseInt(lgK), DEFAULT_UPDATE_SEED);
  }

  /**
   * Constructor for the intermediate pass of an Algebraic function. Pig will call
   * this and pass the same constructor arguments as the base UDF.
   *
   * @param lgK parameter controlling the sketch size and accuracy
   * @param seed for the hash function
   */
  public DataToSketchAlgebraicIntermediate(final String lgK, final String seed) {
    super(Integer.parseInt(lgK), Long.parseLong(seed));
  }

  @Override
  boolean isInputRaw() {
    return true;
  }

}
