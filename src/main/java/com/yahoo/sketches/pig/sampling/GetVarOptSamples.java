/*
 * Copyright 2017, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.pig.sampling;

import static com.yahoo.sketches.pig.sampling.VarOptCommonImpl.RECORD_ALIAS;
import static com.yahoo.sketches.pig.sampling.VarOptCommonImpl.WEIGHT_ALIAS;
import static com.yahoo.sketches.pig.sampling.VarOptCommonImpl.createDataBagFromSketch;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.sampling.VarOptItemsSketch;

/**
 * This UDF extracts samples from the binary image of a VarOpt&lt;Tuple&gt; sketch. Because the
 * input is a binary object, this UDF is unable to automatically determine the data schema at query
 * planning time, beyond knowing that the result will be a <tt>DataBag</tt> of
 * (varOptWeight, (record)) tuples.
 *
 * @author Jon Malkin
 */
public class GetVarOptSamples extends EvalFunc<DataBag> {
  private static final ArrayOfTuplesSerDe SERDE = new ArrayOfTuplesSerDe();

  @Override
  public DataBag exec(final Tuple inputTuple) throws IOException {
    if (inputTuple == null || inputTuple.size() < 1 || inputTuple.isNull(0)) {
      return null;
    }

    final DataByteArray dba = (DataByteArray) inputTuple.get(0);
    final Memory mem = Memory.wrap(dba.get());
    final VarOptItemsSketch<Tuple> sketch = VarOptItemsSketch.heapify(mem, SERDE);

    return createDataBagFromSketch(sketch);
  }

  @Override
  public Schema outputSchema(final Schema input) {
    try {
      if (input == null || input.size() == 0
              || input.getField(0).type != DataType.BYTEARRAY) {
        throw new IllegalArgumentException("Input to GetVarOptSamples must be a DataByteArray: "
                + (input == null ? "null" : input.toString()));
      }

      final Schema weightedSampleSchema = new Schema();
      weightedSampleSchema.add(new Schema.FieldSchema(WEIGHT_ALIAS, DataType.DOUBLE));
      weightedSampleSchema.add(new Schema.FieldSchema(RECORD_ALIAS, DataType.TUPLE));

      return new Schema(new Schema.FieldSchema(getSchemaName(this
              .getClass().getName().toLowerCase(), input), weightedSampleSchema, DataType.BAG));
    } catch (final FrontendException e) {
      throw new RuntimeException(e);
    }
  }
}
