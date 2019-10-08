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

package org.apache.datasketches.pig.quantiles;

import java.util.Comparator;

import org.apache.datasketches.ArrayOfItemsSerDe;
import org.apache.datasketches.ArrayOfStringsSerDe;

/**
 * Creates an ItemsSketch&lt;String&gt; from raw data.
 * It supports all three ways: exec(), Accumulator and Algebraic.
 */
@SuppressWarnings("javadoc")
public class DataToStringsSketch extends DataToItemsSketch<String> {

  private static final Comparator<String> COMPARATOR = Comparator.naturalOrder();
  private static final ArrayOfItemsSerDe<String> SER_DE = new ArrayOfStringsSerDe();

  public DataToStringsSketch() {
    super(0, COMPARATOR, SER_DE);
  }

  public DataToStringsSketch(final String kStr) {
    super(Integer.parseInt(kStr), COMPARATOR, SER_DE);
  }

  // ALGEBRAIC INTERFACE

  @Override
  public String getInitial() {
    return DataToItemsSketchInitial.class.getName();
  }

  @Override
  public String getIntermed() {
    return DataToStringsSketchIntermediateFinal.class.getName();
  }

  @Override
  public String getFinal() {
    return DataToStringsSketchIntermediateFinal.class.getName();
  }

  public static class DataToStringsSketchIntermediateFinal
      extends DataToItemsSketchIntermediateFinal<String> {

    public DataToStringsSketchIntermediateFinal() {
      super(0, COMPARATOR, SER_DE);
    }

    public DataToStringsSketchIntermediateFinal(final String kStr) {
      super(Integer.parseInt(kStr), COMPARATOR, SER_DE);
    }

  }

}
