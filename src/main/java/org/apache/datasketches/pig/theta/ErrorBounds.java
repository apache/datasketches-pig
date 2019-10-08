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
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

//@formatter:off
/**
 * This is a User Defined Function (UDF) for returning the Double value result along with a lower and
 * upper bound. Refer to {@link DataToSketch#exec(Tuple)} for the definition of a Sketch Tuple.
 *
 * <p>
 * <b>Sketch Result Tuple</b>
 * </p>
 * <ul>
 *   <li>Tuple: TUPLE (Contains 3 fields)
 *     <ul>
 *       <li>index 0: Double: DOUBLE: The Estimation Result</li>
 *       <li>index 1: Double: DOUBLE: The Upper Bound of the Estimation Result at 95.4% confidence.</li>
 *       <li>index 2: Double: DOUBLE: The Lower Bound of the Estimation Result at 95.4% confidence.</li>
 *     </ul>
 *   </li>
 * </ul>
 */
//@formatter:on
public class ErrorBounds extends EvalFunc<Tuple> {
  private static final TupleFactory tupleFactory = TupleFactory.getInstance();
  private final long seed_;

  /**
   * Constructs with the DEFAULT_UPDATE_SEED used when deserializing the sketch.
   */
  public ErrorBounds() {
    this(Util.DEFAULT_UPDATE_SEED);
  }

  /**
   * Constructs with the given seed.
   * @param seedStr the string seed used when deserializing the sketch.
   */
  public ErrorBounds(final String seedStr) {
    this(Long.parseLong(seedStr));
  }

  /**
   * Constructs with the given seed.
   * @param seed used when deserializing the sketch.
   */
  public ErrorBounds(final long seed) {
    super();
    seed_ = seed;
  }

  @Override
  public Tuple exec(final Tuple sketchTuple) throws IOException { //throws is in API
    if ((sketchTuple == null) || (sketchTuple.size() == 0)) {
      return null;
    }
    final Sketch sketch = tupleToSketch(sketchTuple, seed_);
    final Tuple outputTuple = tupleFactory.newTuple(3);
    outputTuple.set(0, Double.valueOf(sketch.getEstimate()));
    outputTuple.set(1, Double.valueOf(sketch.getUpperBound(2)));
    outputTuple.set(2, Double.valueOf(sketch.getLowerBound(2)));
    return outputTuple;
  }

  /**
   * The output is a Sketch Result Tuple Schema.
   */
  @Override
  public Schema outputSchema(final Schema input) {
    if (input != null) {
      try {
        final Schema tupleSchema = new Schema();
        tupleSchema.add(new Schema.FieldSchema("Estimate", DataType.DOUBLE));
        tupleSchema.add(new Schema.FieldSchema("UpperBound", DataType.DOUBLE));
        tupleSchema.add(new Schema.FieldSchema("LowerBound", DataType.DOUBLE));
        return new Schema(new Schema.FieldSchema(getSchemaName(this
            .getClass().getName().toLowerCase(), input), tupleSchema, DataType.TUPLE));
      }
      catch (final FrontendException e) {
        // fall through
      }
    }
    return null;
  }

}
