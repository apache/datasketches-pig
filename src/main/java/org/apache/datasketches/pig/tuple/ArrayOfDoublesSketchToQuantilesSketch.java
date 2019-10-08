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

import java.io.IOException;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.quantiles.DoublesSketch;
import org.apache.datasketches.quantiles.DoublesSketchBuilder;
import org.apache.datasketches.quantiles.UpdateDoublesSketch;
import org.apache.datasketches.tuple.ArrayOfDoublesSketch;
import org.apache.datasketches.tuple.ArrayOfDoublesSketchIterator;
import org.apache.datasketches.tuple.ArrayOfDoublesSketches;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

/**
 * This UDF converts a given column of double values from an ArrayOfDoubles sketch
 * to a quantiles DoublesSketch to further analyze the distribution of these values.
 * The result will be a DataByteArray with serialized quantiles sketch.
 */
public class ArrayOfDoublesSketchToQuantilesSketch extends EvalFunc<DataByteArray> {

  private final int k;

  /**
   * Constructor with default parameter k for quantiles sketch
   */
  public ArrayOfDoublesSketchToQuantilesSketch() {
    k = 0;
  }

  /**
   * Constructor with a given parameter k for quantiles sketch
   * @param k string representation of the parameter k that determines the accuracy
   *   and size of the quantiles sketch
   */
  public ArrayOfDoublesSketchToQuantilesSketch(final String k) {
    this.k = Integer.parseInt(k);
  }

  @Override
  public DataByteArray exec(final Tuple input) throws IOException {
    if ((input == null) || (input.size() == 0)) {
      return null;
    }

    final DataByteArray dba = (DataByteArray) input.get(0);
    final ArrayOfDoublesSketch sketch = ArrayOfDoublesSketches.wrapSketch(Memory.wrap(dba.get()));

    int column = 1;
    if (input.size() > 1) {
      column = (int) input.get(1);
      if ((column < 1) || (column > sketch.getNumValues())) {
        throw new IllegalArgumentException("Column number out of range. The given sketch has "
          + sketch.getNumValues() + " columns");
      }
    }

    final DoublesSketchBuilder builder = DoublesSketch.builder();
    if (k > 0) {
      builder.setK(k);
    }
    final UpdateDoublesSketch qs = builder.build();
    final ArrayOfDoublesSketchIterator it = sketch.iterator();
    while (it.next()) {
      qs.update(it.getValues()[column - 1]);
    }
    return new DataByteArray(qs.compact().toByteArray());
  }

}
