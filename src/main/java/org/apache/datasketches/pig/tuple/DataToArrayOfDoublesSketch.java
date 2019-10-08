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

import org.apache.pig.Algebraic;

/**
 * This UDF creates an ArrayOfDoublesSketch from raw data.
 * It supports all three ways: exec(), Accumulator and Algebraic.
 */
@SuppressWarnings("javadoc")
public class DataToArrayOfDoublesSketch extends DataToArrayOfDoublesSketchBase implements Algebraic {

  /**
   * Constructor with default sketch size, default sampling probability of 1
   * and default number of values per key, which is 1.
   */
  public DataToArrayOfDoublesSketch() {
    super();
  }

  /**
   * Constructor with default sketch size and default sampling probability of 1.
   * @param numValues Number of double values to keep for each key
   */
  public DataToArrayOfDoublesSketch(final String numValues) {
    super(Integer.parseInt(numValues));
  }

  /**
   * Constructor with given sketch size, number of values and default sampling probability of 1.
   * @param sketchSize String representation of sketch size
   * @param numValues Number of double values to keep for each key
   */
  public DataToArrayOfDoublesSketch(final String sketchSize, final String numValues) {
    super(Integer.parseInt(sketchSize), Integer.parseInt(numValues));
  }

  /**
   * Constructor with given sketch size, sampling probability and number of values.
   * @param sketchSize String representation of sketch size
   * @param samplingProbability probability from 0 to 1
   * @param numValues Number of double values to keep for each key
   */
  public DataToArrayOfDoublesSketch(
      final String sketchSize, final String samplingProbability, final String numValues) {
    super(Integer.parseInt(sketchSize), Float.parseFloat(samplingProbability),
        Integer.parseInt(numValues));
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
     * Using default parameters.
     */
    public Initial() {}

    /**
     * Constructor for the initial pass of an Algebraic function. This will be passed the same
     * constructor arguments as the original UDF.
     * @param numValues Number of double values to keep for each key
     */
    public Initial(final String numValues) {}

    /**
     * Constructor for the initial pass of an Algebraic function. This will be passed the same
     * constructor arguments as the original UDF.
     * @param sketchSize String representation of sketch size
     * @param numValues Number of double values to keep for each key
     */
    public Initial(final String sketchSize, final String numValues) {}

    /**
     * Constructor for the initial pass of an Algebraic function. This will be passed the same
     * constructor arguments as the original UDF.
     * @param sketchSize String representation of sketch size
     * @param samplingProbability probability from 0 to 1
     * @param numValues Number of double values to keep for each key
     */
    public Initial(final String sketchSize, final String samplingProbability, final String numValues) {}

  }

  public static class IntermediateFinal extends DataToArrayOfDoublesSketchAlgebraicIntermediateFinal {
    /**
     * Constructor for the intermediate and final passes of an Algebraic function.
     * Using default parameters.
     */
    public IntermediateFinal() {
      super();
    }

    /**
     * Constructor for the intermediate and final passes of an Algebraic function. This will be
     * passed the same constructor arguments as the original UDF.
     * @param numValues Number of double values to keep for each key
     */
    public IntermediateFinal(final String numValues) {
      super(Integer.parseInt(numValues));
    }

    /**
     * Constructor for the intermediate and final passes of an Algebraic function. This will be
     * passed the same constructor arguments as the original UDF.
     * @param sketchSize String representation of sketch size
     * @param numValues Number of double values to keep for each key
     */
    public IntermediateFinal(final String sketchSize, final String numValues) {
      super(Integer.parseInt(sketchSize), Integer.parseInt(numValues));
    }

    /**
     * Constructor for the intermediate and final passes of an Algebraic function. This will be
     * passed the same constructor arguments as the original UDF.
     * @param sketchSize String representation of sketch size
     * @param samplingProbability probability from 0 to 1
     * @param numValues Number of double values to keep for each key
     */
    public IntermediateFinal(
        final String sketchSize, final String samplingProbability, final String numValues) {
      super(Integer.parseInt(sketchSize), Float.parseFloat(samplingProbability),
          Integer.parseInt(numValues));
    }

  }

}
