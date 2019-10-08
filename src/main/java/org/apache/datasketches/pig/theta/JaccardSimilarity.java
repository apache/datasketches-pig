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

import static org.apache.datasketches.Util.DEFAULT_UPDATE_SEED;
import static org.apache.datasketches.pig.theta.PigUtil.extractFieldAtIndex;

import java.io.IOException;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.Sketch;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

/**
 * This is a Pig UDF that performs the JaccardSimilarity Operation on two given
 * Sketches.
 *
 * @author eshcar
 */
public class JaccardSimilarity extends EvalFunc<Tuple> {

  private final long seed;

  //TOP LEVEL API
  /**
   * Default constructor to make pig validation happy.  Assumes:
   * <ul>
   * <li><a href="{@docRoot}/resources/dictionary.html#defaultUpdateSeed">See Default Update Seed</a></li>
   * </ul>
   */
  public JaccardSimilarity() {
    this(DEFAULT_UPDATE_SEED);
  }

  /**
   * String constructor.
   *
   * @param seedStr <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>
   */
  public JaccardSimilarity(final String seedStr) {
    this(Long.parseLong(seedStr));
  }

  /**
   * Base constructor.
   *
   * @param seed  <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>.
   */
  public JaccardSimilarity(final long seed) {
    super();
    this.seed = seed;
  }

  // @formatter:off
  /**
   * Top Level Exec Function.
   * <p>
   * This method accepts a <b>Sketch JaccardSimilarityAB Input Tuple</b> and returns a
   * <b>Tuple {LowerBound, Estimate, UpperBound} </b> of the Jaccard ratio.
   * The Upper and Lower bounds are for a confidence interval of 95.4% or +/- 2 standard deviations.
   * </p>
   *
   * <b>Sketch JaccardSimilarityAB Input Tuple</b>
   * <ul>
   *   <li>Tuple: TUPLE (Must contain 2 fields): <br>
   *   Java data type: Pig DataType: Description
   *     <ul>
   *       <li>index 0: DataByteArray: BYTEARRAY: Sketch A</li>
   *       <li>index 1: DataByteArray: BYTEARRAY: Sketch B</li>
   *     </ul>
   *   </li>
   * </ul>
   *
   * <p>
   * Any other input tuple will throw an exception!
   * </p>
   *
   * <b>Tuple {LowerBound, Estimate, UpperBound}</b>
   * <ul>
   *   <li>Tuple: TUPLE (Contains 3 fields)
   *     <ul>
   *       <li>index 0: Double: DOUBLE = The lower bound of the Jaccard Similarity.</li>
   *       <li>index 1: Double: DOUBLE = The estimation of the Jaccard Similarity.</li>
   *       <li>index 2: Double: DOUBLE = The upper bound of the Jaccard Similarity.</li>
   *     </ul>
   *   </li>
   * </ul>
   *
   * @throws ExecException from Pig.
   */
  // @formatter:on

  @Override //TOP LEVEL EXEC
  public Tuple exec(final Tuple inputTuple) throws IOException {
    //The exec is a stateless function.  It operates on the input and returns a result.
    // It can only call static functions.
    final Object objA = extractFieldAtIndex(inputTuple, 0);
    Sketch sketchA = null;
    if (objA != null) {
      final DataByteArray dbaA = (DataByteArray)objA;
      final Memory srcMem = Memory.wrap(dbaA.get());
      sketchA = Sketch.wrap(srcMem, seed);
    }
    final Object objB = extractFieldAtIndex(inputTuple, 1);
    Sketch sketchB = null;
    if (objB != null) {
      final DataByteArray dbaB = (DataByteArray)objB;
      final Memory srcMem = Memory.wrap(dbaB.get());
      sketchB = Sketch.wrap(srcMem, seed);
    }

    final double[] jaccardTupple =
        org.apache.datasketches.theta.JaccardSimilarity.jaccard(sketchA, sketchB);
    return doubleArrayToTuple(jaccardTupple);
  }

  /**
   * Serialize a double array into a Tuple
   *
   * @param doubleArray The doubles array to serialize
   * @return Double Tuple.
   */
  static private Tuple doubleArrayToTuple(final double[] doubleArray) {
    if ((doubleArray == null) || (doubleArray.length == 0)) {
      return null;
    }
    final int arraySize = doubleArray.length;
    final Tuple outputTuple = TupleFactory.getInstance().newTuple(arraySize);
    for (int i = 0; i < arraySize; i++) {
      try {
        outputTuple.set(i, doubleArray[i]);
      }
      catch (final IOException e) {
        throw new IllegalArgumentException("IOException thrown: " + e);
      }
    }
    return outputTuple;
  }

}
