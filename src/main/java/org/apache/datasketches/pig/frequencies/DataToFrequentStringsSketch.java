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

package org.apache.datasketches.pig.frequencies;

import org.apache.datasketches.ArrayOfStringsSerDe;
import org.apache.pig.Algebraic;

/**
 * This UDF creates a FrequentItemsSketch&lt;String&gt; from raw data.
 * It supports all three ways: exec(), Accumulator and Algebraic.
 */
public class DataToFrequentStringsSketch extends DataToFrequentItemsSketch<String> implements Algebraic {

  /**
   * Constructor
   * @param sketchSize String representation of sketch size
   */
  public DataToFrequentStringsSketch(final String sketchSize) {
    super(Integer.parseInt(sketchSize), new ArrayOfStringsSerDe());
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

  @SuppressWarnings("javadoc")
  public static class Initial extends AlgebraicInitial {
    /**
     * Constructor for the initial pass of an Algebraic function. This will be passed the same
     * constructor arguments as the original UDF.
     * @param sketchSize String representation of sketch size
     */
    public Initial(final String sketchSize) {}

    /**
     * Default constructor to make pig validation happy
     */
    public Initial() {}
  }

  @SuppressWarnings("javadoc")
  public static class IntermediateFinal extends DataToFrequentItemsSketchAlgebraicIntermediateFinal<String> {
    /**
     * Constructor for the intermediate and final passes of an Algebraic function. This will be
     * passed the same constructor arguments as the original UDF.
     * @param sketchSize String representation of sketch size
     */
    public IntermediateFinal(final String sketchSize) {
      super(Integer.parseInt(sketchSize), new ArrayOfStringsSerDe());
    }

    /**
     * Default constructor to make pig validation happy.
     */
    public IntermediateFinal() {}
  }

}
