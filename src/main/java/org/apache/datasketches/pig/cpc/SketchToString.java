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
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

/**
 * This is a User Defined Function (UDF) for printing a human-readable summary of a given CpcSketch
 * @author Alexander Saydakov
 */
public class SketchToString extends EvalFunc<String> {

  private final boolean isDetailed_;
  private final long seed_;

  /**
   * Prints only the sketch summary using the default seed.
   */
  public SketchToString() {
    this(false, DEFAULT_UPDATE_SEED);
  }

  /**
   * Prints the summary and details using the default seed.
   *
   * @param isDetailed flag to print the sketch detail
   */
  public SketchToString(final String isDetailed) {
    this(Boolean.parseBoolean(isDetailed), DEFAULT_UPDATE_SEED);
  }

  /**
   * Prints the summary and details using a given seed.
   *
   * @param isDetailed flag to print the sketch detail
   * @param seed parameter for the hash function
   */
  public SketchToString(final String isDetailed, final String seed) {
    this(Boolean.parseBoolean(isDetailed), Long.parseLong(seed));
  }

  /**
   * Internal constructor with primitive parameters.
   *
   * @param isDetailed flag to print the sketch detail
   * @param seed parameter for the hash function
   */
  private SketchToString(final boolean isDetailed, final long seed) {
    isDetailed_ = isDetailed;
    seed_ = seed;
  }

  @Override
  public String exec(final Tuple sketchTuple) throws IOException {
    if ((sketchTuple == null) || (sketchTuple.size() == 0)) {
      return null;
    }
    final DataByteArray dba = (DataByteArray) sketchTuple.get(0);
    final CpcSketch sketch = CpcSketch.heapify(dba.get(), seed_);
    return sketch.toString(isDetailed_);
  }

}
