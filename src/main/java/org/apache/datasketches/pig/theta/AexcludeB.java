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
import static org.apache.datasketches.pig.theta.PigUtil.compactOrderedSketchToTuple;
import static org.apache.datasketches.pig.theta.PigUtil.extractFieldAtIndex;

import java.io.IOException;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.AnotB;
import org.apache.datasketches.theta.CompactSketch;
import org.apache.datasketches.theta.SetOperation;
import org.apache.datasketches.theta.Sketch;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * This is a Pig UDF that performs the A-NOT-B Set Operation on two given Sketches. Because this
 * operation is fundamentally asymmetric, it is structured as a single stateless operation rather
 * than stateful as are Union and Intersection UDFs, which can be iterative.
 * The requirement to perform iterative A\B\C\... is rare. If needed, it can be rendered easily by
 * the caller.
 */
public class AexcludeB extends EvalFunc<Tuple> {
  private final long seed_;

  //TOP LEVEL API
  /**
   * Default constructor to make pig validation happy.  Assumes:
   * <ul>
   * <li><a href="{@docRoot}/resources/dictionary.html#defaultUpdateSeed">See Default Update Seed</a></li>
   * </ul>
   */
  public AexcludeB() {
    this(DEFAULT_UPDATE_SEED);
  }

  /**
   * String constructor.
   *
   * @param seedStr <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>
   */
  public AexcludeB(final String seedStr) {
    this(Long.parseLong(seedStr));
  }

  /**
   * Base constructor.
   *
   * @param seed  <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>.
   */
  public AexcludeB(final long seed) {
    super();
    seed_ = seed;
  }

  // @formatter:off
  /**
   * Top Level Exec Function.
   * <p>
   * This method accepts a <b>Sketch AnotB Input Tuple</b> and returns a
   * <b>Sketch Tuple</b>.
   * </p>
   *
   * <b>Sketch AnotB Input Tuple</b>
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
   * <b>Sketch Tuple</b>
   * <ul>
   *   <li>Tuple: TUPLE (Contains exactly 1 field)
   *     <ul>
   *       <li>index 0: DataByteArray: BYTEARRAY = The serialization of a Sketch object.</li>
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
      sketchA = Sketch.wrap(srcMem, seed_);
    }
    final Object objB = extractFieldAtIndex(inputTuple, 1);
    Sketch sketchB = null;
    if (objB != null) {
      final DataByteArray dbaB = (DataByteArray)objB;
      final Memory srcMem = Memory.wrap(dbaB.get());
      sketchB = Sketch.wrap(srcMem, seed_);
    }

    final AnotB aNOTb = SetOperation.builder().setSeed(seed_).buildANotB();
    aNOTb.update(sketchA, sketchB);
    final CompactSketch compactSketch = aNOTb.getResult(true, null);
    return compactOrderedSketchToTuple(compactSketch);
  }

  @Override
  public Schema outputSchema(final Schema input) {
    if (input != null) {
      try {
        final Schema tupleSchema = new Schema();
        tupleSchema.add(new Schema.FieldSchema("Sketch", DataType.BYTEARRAY));
        return new Schema(new Schema.FieldSchema(getSchemaName(this
            .getClass().getName().toLowerCase(), input), tupleSchema, DataType.TUPLE));
      }
      catch (final FrontendException e) {
        // fall through
      }
    }
    return null;
  }

}
