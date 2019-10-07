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
 * This is a User Defined Function (UDF) for getting a distinct count estimate from a given CpcdSketch
 *
 * @author Alexander Saydakov
 */
public class GetEstimate extends EvalFunc<Double> {

  private final long seed_;

  /**
   * Constructor with default seed
   */
  public GetEstimate() {
    this(DEFAULT_UPDATE_SEED);
  }

  /**
   * Constructor with given seed
   * @param seed in a form of a String
   */
  public GetEstimate(final String seed) {
    this(Long.parseLong(seed));
  }

  /**
   * Base constructor
   * @param seed parameter for the hash function
   */
  GetEstimate(final long seed) {
    seed_ = seed;
  }

  @Override
  public Double exec(final Tuple sketchTuple) throws IOException {
    if ((sketchTuple == null) || (sketchTuple.size() == 0)) {
      return null;
    }
    final DataByteArray dba = (DataByteArray) sketchTuple.get(0);
    final CpcSketch sketch = CpcSketch.heapify(dba.get(), seed_);
    return sketch.getEstimate();
  }

}
