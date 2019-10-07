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
 * Computes union of ItemsSketch&lt;String&gt;.
 * It supports all three ways: exec(), Accumulator and Algebraic
 */
@SuppressWarnings("javadoc")
public class UnionStringsSketch extends UnionItemsSketch<String> {

  private static final Comparator<String> COMPARATOR = Comparator.naturalOrder();
  private static final ArrayOfItemsSerDe<String> SER_DE = new ArrayOfStringsSerDe();

  public UnionStringsSketch() {
    super(0, COMPARATOR, SER_DE);
  }

  public UnionStringsSketch(final String kStr) {
    super(Integer.parseInt(kStr), COMPARATOR, SER_DE);
  }

  //ALGEBRAIC INTERFACE

  @Override
  public String getInitial() {
    return UnionItemsSketchInitial.class.getName();
  }

  @Override
  public String getIntermed() {
    return UnionStringsSketchIntermediateFinal.class.getName();
  }

  @Override
  public String getFinal() {
    return UnionStringsSketchIntermediateFinal.class.getName();
  }

  public static class UnionStringsSketchIntermediateFinal extends UnionItemsSketchIntermediateFinal<String> {

    public UnionStringsSketchIntermediateFinal() {
      super(0, COMPARATOR, SER_DE);
    }

    public UnionStringsSketchIntermediateFinal(final String kStr) {
      super(Integer.parseInt(kStr), COMPARATOR, SER_DE);
    }

  }

}
