/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.pig.frequencies;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.ArrayOfStringsSerDe;
import com.yahoo.sketches.frequencies.ErrorType;
import com.yahoo.sketches.frequencies.ItemsSketch;

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
        ItemsSketch.getInstance(new NativeMemory(dba.get()), new ArrayOfStringsSerDe());
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
