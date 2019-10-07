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

package org.apache.datasketches.pig.quantiles;

import org.apache.datasketches.ArrayOfItemsSerDe;
import org.apache.datasketches.ArrayOfStringsSerDe;
import org.apache.datasketches.quantiles.ItemsSketch;

import java.util.Arrays;
import java.util.Comparator;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.TupleFactory;

import org.testng.annotations.Test;
import org.testng.Assert;

@SuppressWarnings("javadoc")
public class GetQuantileFromStringsSketchTest {

  private static final TupleFactory TUPLE_FACTORY = TupleFactory.getInstance();

  private static final Comparator<String> COMPARATOR = Comparator.naturalOrder();
  private static final ArrayOfItemsSerDe<String> SER_DE = new ArrayOfStringsSerDe();

  @Test
  public void emptySketch() throws Exception {
    EvalFunc<String> func = new GetQuantileFromStringsSketch();
    ItemsSketch<String> sketch = ItemsSketch.getInstance(COMPARATOR);
    String result = func.exec(TUPLE_FACTORY.newTuple(Arrays.asList(new DataByteArray(sketch.toByteArray(SER_DE)), 0.0)));
    Assert.assertNull(result);
  }

  @Test
  public void normalCase() throws Exception {
    EvalFunc<String> func = new GetQuantileFromStringsSketch();
    ItemsSketch<String> sketch = ItemsSketch.getInstance(COMPARATOR);
    sketch.update("a");
    String result = func.exec(TUPLE_FACTORY.newTuple(Arrays.asList(new DataByteArray(sketch.toByteArray(SER_DE)), 0.5)));
    Assert.assertEquals(result, "a");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void wrongNumberOfInputs() throws Exception {
    EvalFunc<String> func = new GetQuantileFromStringsSketch();
    func.exec(TUPLE_FACTORY.newTuple(1));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void wrongTypeForSketch() throws Exception {
    EvalFunc<String> func = new GetQuantileFromStringsSketch();
    func.exec(TUPLE_FACTORY.newTuple(Arrays.asList(1.0, 1.0)));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void wrongTypeForFraction() throws Exception {
    EvalFunc<String> func = new GetQuantileFromStringsSketch();
    ItemsSketch<String> sketch = ItemsSketch.getInstance(COMPARATOR);
    func.exec(TUPLE_FACTORY.newTuple(Arrays.asList(new DataByteArray(sketch.toByteArray(SER_DE)), 1)));
  }
}
