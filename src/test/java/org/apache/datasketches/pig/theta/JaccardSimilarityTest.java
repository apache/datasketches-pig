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

package org.apache.datasketches.pig.theta;

import static org.apache.datasketches.pig.PigTestingUtil.createDbaFromQssRange;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.testng.annotations.Test;

@SuppressWarnings("javadoc")
public class JaccardSimilarityTest {

  @Test
  public void checkNullCombinations() throws IOException {
    EvalFunc<Tuple> jaccardFunc = new JaccardSimilarity();

    Tuple inputTuple, resultTuple;
    //Two nulls
    inputTuple = TupleFactory.getInstance().newTuple(2);
    resultTuple = jaccardFunc.exec(inputTuple);
    assertNotNull(resultTuple);
    assertEquals(resultTuple.size(), 3);
    for (Object d : resultTuple.getAll()) {
      assertEquals(d, 0.0);
    }

    //A is null
    inputTuple = TupleFactory.getInstance().newTuple(2);
    inputTuple.set(1, createDbaFromQssRange(256, 0, 128));
    resultTuple = jaccardFunc.exec(inputTuple);
    assertNotNull(resultTuple);
    assertEquals(resultTuple.size(), 3);
    for (Object d : resultTuple.getAll()) {
      assertEquals(d, 0.0);
    }

    //A is valid, B is null
    inputTuple = TupleFactory.getInstance().newTuple(2);
    inputTuple.set(0, createDbaFromQssRange(256, 0, 256));
    resultTuple = jaccardFunc.exec(inputTuple);
    assertNotNull(resultTuple);
    assertEquals(resultTuple.size(), 3);
    for (Object d : resultTuple.getAll()) {
      assertEquals(d, 0.0);
    }

    //Both valid
    inputTuple = TupleFactory.getInstance().newTuple(2);
    inputTuple.set(0, createDbaFromQssRange(256, 0, 256));
    inputTuple.set(1, createDbaFromQssRange(256, 0, 128));
    resultTuple = jaccardFunc.exec(inputTuple);
    assertNotNull(resultTuple);
    assertEquals(resultTuple.size(), 3);
    assertEquals(resultTuple.get(0), 0.5);
    assertEquals(resultTuple.get(1), 0.5);
    assertEquals(resultTuple.get(2), 0.5);
  }

}
