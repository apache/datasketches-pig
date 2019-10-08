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
public class GetKFromStringsSketchTest {

  private static final TupleFactory TUPLE_FACTORY = TupleFactory.getInstance();

  private static final Comparator<String> COMPARATOR = Comparator.naturalOrder();
  private static final ArrayOfItemsSerDe<String> SER_DE = new ArrayOfStringsSerDe();

  @Test
  public void defalutK() throws Exception {
    EvalFunc<Integer> func = new GetKFromStringsSketch();
    ItemsSketch<String> sketch = ItemsSketch.getInstance(COMPARATOR);
    Integer result = func.exec(TUPLE_FACTORY.newTuple(Arrays.asList(new DataByteArray(sketch.toByteArray(SER_DE)))));
    Assert.assertNotNull(result);
    Assert.assertEquals(result, Integer.valueOf(128));
  }

  @Test
  public void customK() throws Exception {
    EvalFunc<Integer> func = new GetKFromStringsSketch();
    ItemsSketch<String> sketch = ItemsSketch.getInstance(1024, COMPARATOR);
    Integer result = func.exec(TUPLE_FACTORY.newTuple(Arrays.asList(new DataByteArray(sketch.toByteArray(SER_DE)))));
    Assert.assertNotNull(result);
    Assert.assertEquals(result, Integer.valueOf(1024));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void tooFewInputs() throws Exception {
    EvalFunc<Integer> func = new GetKFromStringsSketch();
    func.exec(TUPLE_FACTORY.newTuple());
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void tooManyInputs() throws Exception {
    EvalFunc<Integer> func = new GetKFromStringsSketch();
    func.exec(TUPLE_FACTORY.newTuple(2));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void wrongTypeForSketch() throws Exception {
    EvalFunc<Integer> func = new GetKFromStringsSketch();
    func.exec(TUPLE_FACTORY.newTuple(Arrays.asList(1.0)));
  }

}
