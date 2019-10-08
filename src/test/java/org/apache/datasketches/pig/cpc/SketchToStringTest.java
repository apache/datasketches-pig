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

package org.apache.datasketches.pig.cpc;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.TupleFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.datasketches.cpc.CpcSketch;

@SuppressWarnings("javadoc")
public class SketchToStringTest {

  private static final TupleFactory TUPLE_FACTORY = TupleFactory.getInstance();

  @Test
  public void nullInputTuple() throws Exception {
    final EvalFunc<String> func = new SketchToString();
    final String result = func.exec(null);
    Assert.assertNull(result);
  }

  @Test
  public void emptyInputTuple() throws Exception {
    final EvalFunc<String> func = new SketchToString();
    final String result = func.exec(TUPLE_FACTORY.newTuple());
    Assert.assertNull(result);
  }

  @Test
  public void normalCase() throws Exception {
    final EvalFunc<String> func = new SketchToString();
    final CpcSketch sketch = new CpcSketch();
    final String result = func.exec(TUPLE_FACTORY.newTuple(new DataByteArray(sketch.toByteArray())));
    Assert.assertNotNull(result);
    Assert.assertTrue(result.length() > 0);
  }

  @Test
  public void normalCaseWithDetail() throws Exception {
    final EvalFunc<String> func = new SketchToString("true");
    final CpcSketch sketch = new CpcSketch();
    final String result = func.exec(TUPLE_FACTORY.newTuple(new DataByteArray(sketch.toByteArray())));
    Assert.assertNotNull(result);
    Assert.assertTrue(result.length() > 0);
  }

  @Test
  public void normalCaseWithDetailAndSeed() throws Exception {
    final EvalFunc<String> func = new SketchToString("true", "123");
    final CpcSketch sketch = new CpcSketch(12, 123);
    final String result = func.exec(TUPLE_FACTORY.newTuple(new DataByteArray(sketch.toByteArray())));
    Assert.assertNotNull(result);
    Assert.assertTrue(result.length() > 0);
  }

}
