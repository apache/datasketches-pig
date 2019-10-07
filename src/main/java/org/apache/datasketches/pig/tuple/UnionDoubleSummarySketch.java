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

package org.apache.datasketches.pig.tuple;

import org.apache.datasketches.tuple.adouble.DoubleSummary;
import org.apache.datasketches.tuple.adouble.DoubleSummaryDeserializer;
import org.apache.datasketches.tuple.adouble.DoubleSummarySetOperations;
import org.apache.pig.Algebraic;

/**
 * This is to union Sketch&lt;DoubleSummary&gt;.
 * It supports all three ways: exec(), Accumulator and Algebraic
 */
@SuppressWarnings("javadoc")
public class UnionDoubleSummarySketch extends UnionSketch<DoubleSummary> implements Algebraic {

  /**
   * Constructor with default sketch size and default mode (sum)
   */
  public UnionDoubleSummarySketch() {
    super(new DoubleSummarySetOperations(DoubleSummary.Mode.Sum),
        new DoubleSummaryDeserializer());
  }

  /**
   * Constructor with default mode (sum)
   * @param sketchSize String representation of sketch size
   */
  public UnionDoubleSummarySketch(final String sketchSize) {
    super(Integer.parseInt(sketchSize),
        new DoubleSummarySetOperations(DoubleSummary.Mode.Sum), new DoubleSummaryDeserializer());
  }

  /**
   * Constructor
   * @param sketchSize String representation of sketch size
   * @param summaryMode String representation of mode (sum, min or max)
   */
  public UnionDoubleSummarySketch(final String sketchSize, final String summaryMode) {
    super(Integer.parseInt(sketchSize),
        new DoubleSummarySetOperations(DoubleSummary.Mode.valueOf(summaryMode)),
        new DoubleSummaryDeserializer());
  }

  @Override
  public String getInitial() {
    return Initial.class.getName();
  }

  @Override
  public String getIntermed() {
    return IntermediateFinal.class.getName();
  }

  @Override
  public String getFinal() {
    return IntermediateFinal.class.getName();
  }

  public static class Initial extends AlgebraicInitial {
    /**
     * Constructor for the initial pass of an Algebraic function.
     * Default sketch size and default mode.
     */
    public Initial() {}

    /**
     * Constructor for the initial pass of an Algebraic function. This will be passed the same
     * constructor arguments as the original UDF.
     * @param sketchSize String representation of sketch size
     */
    public Initial(final String sketchSize) {}

    /**
     * Constructor for the initial pass of an Algebraic function. This will be passed the same
     * constructor arguments as the original UDF.
     * @param sketchSize String representation of sketch size
     * @param summaryMode String representation of mode (sum, min or max)
     */
    public Initial(final String sketchSize, final String summaryMode) {}
  }

  public static class IntermediateFinal extends UnionSketchAlgebraicIntermediateFinal<DoubleSummary> {
    /**
     * Constructor for the intermediate and final passes of an Algebraic function.
     * Default sketch size and default mode.
     */
    public IntermediateFinal() {
      super(new DoubleSummarySetOperations(DoubleSummary.Mode.Sum), new DoubleSummaryDeserializer());
    }

    /**
     * Constructor for the intermediate and final passes of an Algebraic function. This will be
     * passed the same constructor arguments as the original UDF.
     * @param sketchSize String representation of sketch size
     */
    public IntermediateFinal(final String sketchSize) {
      super(Integer.parseInt(sketchSize),
          new DoubleSummarySetOperations(DoubleSummary.Mode.Sum), new DoubleSummaryDeserializer());
    }

    /**
     * Constructor for the intermediate and final passes of an Algebraic function. This will be
     * passed the same constructor arguments as the original UDF.
     * @param sketchSize String representation of sketch size
     * @param summaryMode String representation of mode (sum, min or max)
     */
    public IntermediateFinal(final String sketchSize, final String summaryMode) {
      super(Integer.parseInt(sketchSize),
          new DoubleSummarySetOperations(DoubleSummary.Mode.valueOf(summaryMode)),
          new DoubleSummaryDeserializer());
    }
  }

}
