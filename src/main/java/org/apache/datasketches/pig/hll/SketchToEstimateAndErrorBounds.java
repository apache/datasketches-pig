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

package org.apache.datasketches.pig.hll;

import java.io.IOException;

import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.memory.Memory;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * This is a User Defined Function (UDF) for obtaining the unique count estimate
 * along with a lower and upper bound from an HllSketch.
 *
 * <p>The result is a tuple with three double values: estimate, lower bound and upper bound.
 * The bounds are given at 95.5% confidence.
 *
 * @author Alexander Saydakov
 */
public class SketchToEstimateAndErrorBounds extends EvalFunc<Tuple> {

  @Override
  public Tuple exec(final Tuple sketchTuple) throws IOException {
    if ((sketchTuple == null) || (sketchTuple.size() == 0)) {
      return null;
    }
    final DataByteArray dba = (DataByteArray) sketchTuple.get(0);
    final HllSketch sketch = HllSketch.wrap(Memory.wrap(dba.get()));
    final Tuple outputTuple = TupleFactory.getInstance().newTuple(3);
    outputTuple.set(0, Double.valueOf(sketch.getEstimate()));
    outputTuple.set(1, Double.valueOf(sketch.getLowerBound(2)));
    outputTuple.set(2, Double.valueOf(sketch.getUpperBound(2)));
    return outputTuple;
  }

  /**
   * The output is a Sketch Result Tuple Schema.
   */
  @Override
  public Schema outputSchema(final Schema input) {
    if (input == null) { return null; }
    try {
      final Schema tupleSchema = new Schema();
      tupleSchema.add(new Schema.FieldSchema("Estimate", DataType.DOUBLE));
      tupleSchema.add(new Schema.FieldSchema("LowerBound", DataType.DOUBLE));
      tupleSchema.add(new Schema.FieldSchema("UpperBound", DataType.DOUBLE));
      return new Schema(new Schema.FieldSchema(getSchemaName(this
          .getClass().getName().toLowerCase(), input), tupleSchema, DataType.TUPLE));
    } catch (final FrontendException e) {
      throw new RuntimeException(e);
    }
  }

}
