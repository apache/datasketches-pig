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

import java.io.IOException;
import java.util.Comparator;

import org.apache.datasketches.ArrayOfStringsSerDe;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.quantiles.ItemsSketch;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

/**
 * This UDF is to get the parameter K from a given sketch.
 * This can be useful for debugging a work flow to make sure that resulting sketches
 * have the intended K, and, therefore, the accuracy
 */
public class GetKFromStringsSketch extends EvalFunc<Integer> {

  @Override
  public Integer exec(final Tuple input) throws IOException {
    if (input.size() != 1) {
      throw new IllegalArgumentException("expected one input");
    }

    if (!(input.get(0) instanceof DataByteArray)) {
      throw new IllegalArgumentException("expected a DataByteArray as a sketch, got "
          + input.get(0).getClass().getSimpleName());
    }
    final DataByteArray dba = (DataByteArray) input.get(0);
    final ItemsSketch<String> sketch =
        ItemsSketch.getInstance(Memory.wrap(dba.get()), Comparator.naturalOrder(),
            new ArrayOfStringsSerDe());

    return sketch.getK();
  }

}
