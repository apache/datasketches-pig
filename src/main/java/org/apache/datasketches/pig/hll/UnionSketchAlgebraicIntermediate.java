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

import static org.apache.datasketches.pig.hll.DataToSketch.DEFAULT_HLL_TYPE;
import static org.apache.datasketches.pig.hll.DataToSketch.DEFAULT_LG_K;

import org.apache.datasketches.hll.TgtHllType;
import org.apache.datasketches.hll.Union;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;

@SuppressWarnings("javadoc")
public class UnionSketchAlgebraicIntermediate extends AlgebraicIntermediate {

  /**
   * Default constructor of the intermediate pass of an Algebraic function.
   * Assumes default lgK and target HLL type.
   */
  public UnionSketchAlgebraicIntermediate() {
    super(DEFAULT_LG_K, DEFAULT_HLL_TYPE);
  }

  /**
   * Constructor for the intermediate pass of an Algebraic function. Pig will call
   * this and pass the same constructor arguments as the base UDF.
   * Assumes default HLL target type.
   *
   * @param lgK in a form of a String
   */
  public UnionSketchAlgebraicIntermediate(final String lgK) {
    super(Integer.parseInt(lgK), DEFAULT_HLL_TYPE);
  }

  /**
   * Constructor for the intermediate pass of an Algebraic function. Pig will call
   * this and pass the same constructor arguments as the base UDF.
   *
   * @param lgK parameter controlling the sketch size and accuracy
   * @param tgtHllType HLL type of the resulting sketch
   */
  public UnionSketchAlgebraicIntermediate(final String lgK, final String tgtHllType) {
    super(Integer.parseInt(lgK), TgtHllType.valueOf(tgtHllType));
  }

  @Override
  void updateUnion(final DataBag bag, final Union union) throws ExecException {
    UnionSketch.updateUnion(bag, union);
  }

}
