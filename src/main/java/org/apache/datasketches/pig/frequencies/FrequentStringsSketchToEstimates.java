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

import java.io.IOException;

import org.apache.datasketches.ArrayOfStringsSerDe;
import org.apache.datasketches.frequencies.ErrorType;
import org.apache.datasketches.frequencies.ItemsSketch;
import org.apache.datasketches.memory.Memory;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * This UDF converts a FrequentItemsSketch&lt;String&gt; to estimates:
 * {(item, estimate, upper bound, lower bound), ...}
 */
public class FrequentStringsSketchToEstimates extends EvalFunc<DataBag> {

  private final ErrorType errorType;

  /**
   * Instantiate UDF with default error type
   */
  public FrequentStringsSketchToEstimates() {
    this.errorType = ErrorType.NO_FALSE_POSITIVES;
  }

  /**
   * Instantiate UDF with given error type
   * @param errorType string representation of error type
   */
  public FrequentStringsSketchToEstimates(final String errorType) {
    this.errorType = ErrorType.valueOf(errorType);
  }

  @Override
  public DataBag exec(final Tuple input) throws IOException {
    if ((input == null) || (input.size() == 0)) {
      return null;
    }

    final DataByteArray dba = (DataByteArray) input.get(0);
    final ItemsSketch<String> sketch =
        ItemsSketch.getInstance(Memory.wrap(dba.get()), new ArrayOfStringsSerDe());
    final ItemsSketch.Row<String>[] result = sketch.getFrequentItems(errorType);

    final DataBag bag = BagFactory.getInstance().newDefaultBag();
    for (int i = 0; i < result.length; i++) {
      final Tuple tuple = TupleFactory.getInstance().newTuple(4);
      tuple.set(0, result[i].getItem());
      tuple.set(1, result[i].getEstimate());
      tuple.set(2, result[i].getLowerBound());
      tuple.set(3, result[i].getUpperBound());
      bag.add(tuple);
    }
    return bag;
  }

  @Override
  public Schema outputSchema(final Schema inputSchema) {
    final Schema tupleSchema = new Schema();
    tupleSchema.add(new Schema.FieldSchema("item", DataType.CHARARRAY));
    tupleSchema.add(new Schema.FieldSchema("estimate", DataType.LONG));
    tupleSchema.add(new Schema.FieldSchema("lower_bound", DataType.LONG));
    tupleSchema.add(new Schema.FieldSchema("upper_bound", DataType.LONG));
    try {
      final Schema bagSchema = new Schema(new Schema.FieldSchema("item_tuple", tupleSchema, DataType.TUPLE));
      return new Schema(new Schema.FieldSchema("bag_of_item_tuples", bagSchema, DataType.BAG));
    } catch (final FrontendException e) {
      throw new RuntimeException(e);
    }
  }
}
