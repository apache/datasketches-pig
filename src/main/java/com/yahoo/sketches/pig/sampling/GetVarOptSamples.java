/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.pig.sampling;

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

import com.yahoo.memory.Memory;
import com.yahoo.sketches.sampling.VarOptItemsSamples;
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
  // defined for test consistency
  static final String WEIGHT_ALIAS = "vo_weight";
  static final String RECORD_ALIAS = "record";

  private static final BagFactory BAG_FACTORY = BagFactory.getInstance();
  private static final TupleFactory TUPLE_FACTORY = TupleFactory.getInstance();

  private static final ArrayOfTuplesSerDe SERDE = new ArrayOfTuplesSerDe();

  @Override
  public DataBag exec(final Tuple inputTuple) throws IOException {
    if (inputTuple == null || inputTuple.size() < 1 || inputTuple.isNull(0)) {
      return null;
    }

    final DataByteArray dba = (DataByteArray) inputTuple.get(0);
    final Memory mem = Memory.wrap(dba.get());
    final VarOptItemsSketch<Tuple> sketch = VarOptItemsSketch.heapify(mem, SERDE);

    final VarOptItemsSamples<Tuple> samples = sketch.getSketchSamples();
    final DataBag result = BAG_FACTORY.newDefaultBag();

    for (VarOptItemsSamples.WeightedSample s : samples) {
      final Tuple t = TUPLE_FACTORY.newTuple(2);
      t.set(0, s.getWeight());
      t.set(1, s.getItem());
      result.add(t);
    }

    return result;
  }

  @Override
  public Schema outputSchema(final Schema input) {
    try {
      final Schema weightedSampleSchema = new Schema();
      weightedSampleSchema.add(new Schema.FieldSchema(WEIGHT_ALIAS, DataType.DOUBLE));
      weightedSampleSchema.add(new Schema.FieldSchema(RECORD_ALIAS, DataType.TUPLE));

      return new Schema(new Schema.FieldSchema(getSchemaName(this
              .getClass().getName().toLowerCase(), input), weightedSampleSchema, DataType.BAG));
    } catch (final FrontendException e) {
      // fall through
    }

    return null;
  }
}
