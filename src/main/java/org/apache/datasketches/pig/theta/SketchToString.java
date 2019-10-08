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
 * This is a User Defined Function (UDF) for "pretty printing" the summary of a sketch
 * from a Sketch Tuple.
 *
 * <p>
 * Refer to {@link DataToSketch#exec(Tuple)} for the definition of a Sketch Tuple.
 * </p>
 */
public class SketchToString extends EvalFunc<String> {
  private boolean detailOut = false;
  private final long seed_;

  /**
   * Pretty prints only the sketch summary.
   */
  public SketchToString() {
    this(false, Util.DEFAULT_UPDATE_SEED);
  }

  /**
   * Pretty prints all bucket detail plus the sketch summary based on outputDetail.
   *
   * @param outputDetailStr If the first character is a "T" or "t" the output will include the bucket
   * detail. Otherwise only the sketch summary will be output.
   */
  public SketchToString(final String outputDetailStr) {
    this( outputDetailStr.substring(0, 1).equalsIgnoreCase("T"), Util.DEFAULT_UPDATE_SEED);
  }

  /**
   * Pretty prints all bucket detail plus the sketch summary based on outputDetail.
   *
   * @param outputDetailStr If the first character is a "T" or "t" the output will include the bucket
   * detail. Otherwise only the sketch summary will be output.
   * @param seedStr the seed string
   */
  public SketchToString(final String outputDetailStr, final String seedStr) {
    this( outputDetailStr.substring(0, 1).equalsIgnoreCase("T"), Long.parseLong(seedStr));
  }

  /**
   * Pretty prints all bucket detail plus the sketch summary based on outputDetail.
   *
   * @param outputDetail If the first character is a "T" or "t" the output will include the bucket
   * detail. Otherwise only the sketch summary will be output.
   * @param seed the seed string
   */
  public SketchToString(final boolean outputDetail, final long seed) {
    super();
    detailOut = outputDetail;
    seed_ = seed;
  }

  @Override
  public String exec(final Tuple sketchTuple) throws IOException { //throws is in API
    if ((sketchTuple == null) || (sketchTuple.size() == 0)) {
      return null;
    }
    final Sketch sketch = tupleToSketch(sketchTuple, seed_);
    return sketch.toString(true, detailOut, 8, true);
  }

}
