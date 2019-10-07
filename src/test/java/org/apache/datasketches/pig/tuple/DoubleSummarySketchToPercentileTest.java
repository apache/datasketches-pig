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

import org.testng.annotations.Test;
import org.testng.Assert;

import java.util.Arrays;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import org.apache.datasketches.tuple.UpdatableSketch;
import org.apache.datasketches.tuple.UpdatableSketchBuilder;
import org.apache.datasketches.tuple.adouble.DoubleSummary;
import org.apache.datasketches.tuple.adouble.DoubleSummaryFactory;

@SuppressWarnings("javadoc")
public class DoubleSummarySketchToPercentileTest {

  @Test
  public void emptySketch() throws Exception {
    EvalFunc<Double> func = new DoubleSummarySketchToPercentile();
    UpdatableSketch<Double, DoubleSummary> sketch =
        new UpdatableSketchBuilder<>(new DoubleSummaryFactory(DoubleSummary.Mode.Sum)).build();
    Tuple inputTuple = TupleFactory.getInstance().
        newTuple(Arrays.asList(new DataByteArray(sketch.compact().toByteArray()), 0.0));
    double result = func.exec(inputTuple);
    Assert.assertEquals(result, Double.NaN);
  }

  @Test
  public void normalCase() throws Exception {
    EvalFunc<Double> func = new DoubleSummarySketchToPercentile();
    UpdatableSketch<Double, DoubleSummary> sketch =
        new UpdatableSketchBuilder<>(new DoubleSummaryFactory(DoubleSummary.Mode.Sum)).build();
    int iterations = 100000;
    for (int i = 0; i < iterations; i++) {
      sketch.update(i, (double) i);
    }
    for (int i = 0; i < iterations; i++) {
      sketch.update(i, (double) i);
    }
    Tuple inputTuple = PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray()), 50.0);
    double result = func.exec(inputTuple);
    Assert.assertEquals(result, iterations, iterations * 0.02);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void wrongNumberOfInputs() throws Exception {
    EvalFunc<Double> func = new DoubleSummarySketchToPercentile();
    func.exec(PigUtil.objectsToTuple(1.0));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void percentileOutOfRange() throws Exception {
    EvalFunc<Double> func = new DoubleSummarySketchToPercentile();
    UpdatableSketch<Double, DoubleSummary> sketch =
        new UpdatableSketchBuilder<>(new DoubleSummaryFactory(DoubleSummary.Mode.Sum)).build();
    func.exec(PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray()), 200.0));
  }
}
