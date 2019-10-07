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

import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.memory.Memory;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

/**
 * This is a User Defined Function (UDF) for "pretty printing" the summary of an HllSketch
 *
 * @author Alexander Saydakov
 */
public class SketchToString extends EvalFunc<String> {

  private final boolean hllDetail_;
  private final boolean auxDetail_;

  /**
   * Prints only the sketch summary.
   */
  public SketchToString() {
    this(false, false);
  }

  /**
   * Prints the summary and details based on given input parameters.
   *
   * @param hllDetail flag to print the HLL sketch detail
   * @param auxDetail flag to print the auxiliary detail
   */
  public SketchToString(final String hllDetail, final String auxDetail) {
    this(Boolean.parseBoolean(hllDetail), Boolean.parseBoolean(auxDetail));
  }

  /**
   * Internal constructor with primitive parameters.
   *
   * @param hllDetail flag to print the HLL sketch detail
   * @param auxDetail flag to print the auxiliary detail
   */
  private SketchToString(final boolean hllDetail, final boolean auxDetail) {
    super();
    hllDetail_ = hllDetail;
    auxDetail_ = auxDetail;
  }

  @Override
  public String exec(final Tuple sketchTuple) throws IOException {
    if ((sketchTuple == null) || (sketchTuple.size() == 0)) {
      return null;
    }
    final DataByteArray dba = (DataByteArray) sketchTuple.get(0);
    final HllSketch sketch = HllSketch.wrap(Memory.wrap(dba.get()));
    return sketch.toString(true, hllDetail_, auxDetail_);
  }

}
