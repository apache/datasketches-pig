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

package org.apache.datasketches.pig.theta;

import static org.apache.datasketches.pig.theta.PigUtil.tupleToSketch;

import java.io.IOException;

import org.apache.datasketches.Util;
import org.apache.datasketches.theta.Sketch;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

/**
 * Returns the unique count estimate of a sketch as a Double.
 */
public class Estimate extends EvalFunc<Double> {
  private final long seed_;

  /**
   * Constructs with the DEFAULT_UPDATE_SEED used when deserializing the sketch.
   */
  public Estimate() {
    this(Util.DEFAULT_UPDATE_SEED);
  }

  /**
   * Constructs with the given seed.
   * @param seedStr the string seed used when deserializing the sketch.
   */
  public Estimate(final String seedStr) {
    this(Long.parseLong(seedStr));
  }

  /**
   * Constructs with the given seed.
   * @param seed used when deserializing the sketch.
   */
  public Estimate(final long seed) {
    super();
    this.seed_ = seed;
  }

  @Override
  public Double exec(final Tuple sketchTuple) throws IOException { //throws is in API
    if ((sketchTuple == null) || (sketchTuple.size() == 0)) {
      return null;
    }
    final Sketch sketch =  tupleToSketch(sketchTuple, this.seed_);
    return sketch.getEstimate();
  }
}
