/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.pig.tuple;

import java.util.Arrays;

import org.testng.annotations.Test;
import org.testng.Assert;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

import com.yahoo.sketches.tuple.UpdatableSketch;
import com.yahoo.sketches.tuple.DoubleSummary;
import com.yahoo.sketches.tuple.DoubleSummaryFactory;
import com.yahoo.sketches.tuple.UpdatableSketchBuilder;

public class DoubleSummarySketchToResultTest {
  @Test
  public void emptySketch() throws Exception {
    EvalFunc<Tuple> func = new DoubleSummarySketchToResult();
    UpdatableSketch<Double, DoubleSummary> sketch = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).build();
    Tuple inputTuple = PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray()));
    Tuple resultTuple = func.exec(inputTuple);
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 2);
    Assert.assertEquals(resultTuple.get(0), 0.0); // estimate
    Assert.assertEquals(((Tuple)resultTuple.get(1)).size(), 0);
  }

  @Test
  public void normalCase() throws Exception {
    EvalFunc<Tuple> func = new DoubleSummarySketchToResult();
    UpdatableSketch<Double, DoubleSummary> sketch = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).build();
    sketch.update("a", 1.0);
    sketch.update("b", 1.0);
    sketch.update("a", 2.0);
    sketch.update("b", 2.0);
    Tuple inputTuple = PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray()));
    Tuple resultTuple = func.exec(inputTuple);
    Assert.assertNotNull(resultTuple);
    Assert.assertEquals(resultTuple.size(), 2);
    Assert.assertEquals(resultTuple.get(0), 2.0); // estimate
    Assert.assertEquals(((Tuple)resultTuple.get(1)).size(), 2);
    Assert.assertEquals(((Tuple)resultTuple.get(1)).getAll(), Arrays.asList(3.0, 3.0));
  }
}
