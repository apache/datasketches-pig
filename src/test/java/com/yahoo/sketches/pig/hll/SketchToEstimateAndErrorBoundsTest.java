/*
 * Copyright 2017, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.pig.hll;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.testng.annotations.Test;

import com.yahoo.sketches.hll.HllSketch;

import junit.framework.Assert;

public class SketchToEstimateAndErrorBoundsTest {

  private static final TupleFactory tupleFactory = TupleFactory.getInstance();

  @Test
  public void nullInputTuple() throws Exception {
    EvalFunc<Tuple> func = new SketchToEstimateAndErrorBounds();
    Tuple result = func.exec(null);
    Assert.assertNull(result);
  }

  @Test
  public void emptyInputTuple() throws Exception {
    EvalFunc<Tuple> func = new SketchToEstimateAndErrorBounds();
    Tuple result = func.exec(tupleFactory.newTuple());
    Assert.assertNull(result);
  }

  @Test
  public void normalCase() throws Exception {
    EvalFunc<Tuple> func = new SketchToEstimateAndErrorBounds();
    HllSketch sketch = new HllSketch(12);
    sketch.update(1);
    sketch.update(2);
    Tuple result = func.exec(tupleFactory.newTuple(new DataByteArray(sketch.toCompactByteArray())));
    Assert.assertNotNull(result);
    Assert.assertEquals((Double) result.get(0), 2.0, 0.01);
    Assert.assertTrue((Double) result.get(1) <= 2.0);
    Assert.assertTrue((Double) result.get(2) >= 2.0);
  }

}
