/*
 * Copyright 2019, Verizon Media.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.pig.cpc;

import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.yahoo.sketches.cpc.CpcSketch;

/**
 * This is a User Defined Function (UDF) for obtaining the distinct count estimate
 * along with a lower and upper bound from a given CpcSketch.
 *
 * <p>The result is a tuple with three double values: estimate, lower bound and upper bound.
 * The bounds are best estimates for the confidence interval given <i>kappa</i>, which represents
 * the number of standard deviations from the mean (1, 2 or 3).
 *
 * @author Alexander Saydakov
 */
public class GetEstimateAndErrorBounds extends EvalFunc<Tuple> {

  private static int DEFAULT_KAPPA = 2;

  private final int kappa_;
  private final long seed_;

  /**
   * Constructor with default kappa and seed
   */
  public GetEstimateAndErrorBounds() {
    this(DEFAULT_KAPPA, DEFAULT_UPDATE_SEED);
  }

  /**
   * Constructor with given kappa and default seed
   * @param kappa in a form of a String
   */
  public GetEstimateAndErrorBounds(final String kappa) {
    this(Integer.parseInt(kappa), DEFAULT_UPDATE_SEED);
  }

  /**
   * Constructor with given kappa and seed
   * @param kappa in a form of a String
   * @param seed in a form of a String
   */
  public GetEstimateAndErrorBounds(final String kappa, final String seed) {
    this(Integer.parseInt(kappa), Long.parseLong(seed));
  }

  /**
   * Base constructor
   * @param kappa the given number of standard deviations from the mean: 1, 2 or 3
   * @param seed parameter for the hash function
   */
  GetEstimateAndErrorBounds(final int kappa, final long seed) {
    kappa_ = kappa;
    seed_ = seed;
  }

  @Override
  public Tuple exec(final Tuple sketchTuple) throws IOException {
    if ((sketchTuple == null) || (sketchTuple.size() == 0)) {
      return null;
    }
    final DataByteArray dba = (DataByteArray) sketchTuple.get(0);
    final CpcSketch sketch = CpcSketch.heapify(dba.get(), seed_);
    final Tuple outputTuple = TupleFactory.getInstance().newTuple(3);
    outputTuple.set(0, Double.valueOf(sketch.getEstimate()));
    outputTuple.set(1, Double.valueOf(sketch.getLowerBound(kappa_)));
    outputTuple.set(2, Double.valueOf(sketch.getUpperBound(kappa_)));
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
