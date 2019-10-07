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

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.tuple.Sketch;
import org.apache.datasketches.tuple.Sketches;
import org.apache.datasketches.tuple.Summary;
import org.apache.datasketches.tuple.SummaryDeserializer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

final class Util {

  static final TupleFactory tupleFactory = TupleFactory.getInstance();

  static Tuple doubleArrayToTuple(final double[] array) throws ExecException {
    final Tuple tuple = tupleFactory.newTuple(array.length);
    for (int i = 0; i < array.length; i++) {
      tuple.set(i, array[i]);
    }
    return tuple;
  }

  static <S extends Summary> Sketch<S> deserializeSketchFromTuple(final Tuple tuple,
      final SummaryDeserializer<S> summaryDeserializer) throws ExecException {
    final byte[] bytes = ((DataByteArray) tuple.get(0)).get();
    return Sketches.heapifySketch(Memory.wrap(bytes), summaryDeserializer);
  }

}
