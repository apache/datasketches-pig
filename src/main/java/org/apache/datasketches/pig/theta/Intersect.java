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
import static org.apache.datasketches.pig.theta.PigUtil.emptySketchTuple;
import static org.apache.datasketches.pig.theta.PigUtil.extractBag;
import static org.apache.datasketches.pig.theta.PigUtil.extractFieldAtIndex;
import static org.apache.datasketches.pig.theta.PigUtil.extractTypeAtIndex;

import java.io.IOException;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.CompactSketch;
import org.apache.datasketches.theta.Intersection;
import org.apache.datasketches.theta.SetOperation;
import org.apache.datasketches.theta.Sketch;
import org.apache.pig.Accumulator;
import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * This is a Pig UDF that performs the Intersection Set Operation on Sketches.
 * To assist Pig, this class implements both the <i>Accumulator</i> and <i>Algebraic</i> interfaces.
 */
public class Intersect extends EvalFunc<Tuple> implements Accumulator<Tuple>, Algebraic {
  //With the single exception of the Accumulator interface, UDFs are stateless.
  //All parameters kept at the class level must be final, except for the accumUpdateSketch.
  private final long seed_;
  private final Tuple emptyCompactOrderedSketchTuple_;
  private Intersection accumIntersection_;

  //TOP LEVEL API

  /**
   * Default constructor to make pig validation happy.  Assumes:
   * <ul>
   * <li><a href="{@docRoot}/resources/dictionary.html#defaultUpdateSeed">See Default Update Seed</a></li>
   * </ul>
   */
  public Intersect() {
    this(DEFAULT_UPDATE_SEED);
  }

  /**
   * Full string constructor.
   *
   * @param seedStr  <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>.
   */
  public Intersect(final String seedStr) {
    this(Long.parseLong(seedStr));
  }

  /**
   * Base constructor.
   *
   * @param seed  <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>.
   */
  public Intersect(final long seed) {
    super();
    seed_ = seed;
    emptyCompactOrderedSketchTuple_ = emptySketchTuple(seed);
  }

  //@formatter:off
  /************************************************************************************************
   * Top-level exec function.
   * This method accepts an input Tuple containing a Bag of one or more inner <b>Sketch Tuples</b>
   * and returns a single updated <b>Sketch</b> as a <b>Sketch Tuple</b>.
   *
   * <p>If a large number of calls are anticipated, leveraging either the <i>Algebraic</i> or
   * <i>Accumulator</i> interfaces is recommended. Pig normally handles this automatically.
   *
   * <p>Internally, this method presents the inner <b>Sketch Tuples</b> to a new <b>Intersection</b>.
   * The result is returned as a <b>Sketch Tuple</b>
   *
   * <p><b>Input Tuple</b>
   * <ul>
   *   <li>Tuple: TUPLE (Must contain only one field)
   *     <ul>
   *       <li>index 0: DataBag: BAG (May contain 0 or more Inner Tuples)
   *         <ul>
   *           <li>index 0: Tuple: TUPLE <b>Sketch Tuple</b></li>
   *           <li>...</li>
   *           <li>index n-1: Tuple: TUPLE <b>Sketch Tuple</b></li>
   *         </ul>
   *       </li>
   *     </ul>
   *   </li>
   * </ul>
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
   * @param inputTuple A tuple containing a single bag, containing Sketch Tuples.
   * @return Sketch Tuple. If inputTuple is null or empty, returns empty sketch (8 bytes).
   * @see "org.apache.pig.EvalFunc.exec(org.apache.pig.data.Tuple)"
   */
  //@formatter:on

  @Override //TOP LEVEL EXEC
  public Tuple exec(final Tuple inputTuple) throws IOException { //throws is in API
    //The exec is a stateless function.  It operates on the input and returns a result.
    // It can only call static functions.
    final Intersection intersection = SetOperation.builder().setSeed(seed_).buildIntersection();
    final DataBag bag = extractBag(inputTuple);
    if (bag == null) {
      return emptyCompactOrderedSketchTuple_; //Configured with parent
    }

    updateIntersection(bag, intersection, seed_);
    final CompactSketch compactSketch = intersection.getResult(true, null);
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

  //ACCUMULATOR INTERFACE

  /*************************************************************************************************
   * An <i>Accumulator</i> version of the standard <i>exec()</i> method. Like <i>exec()</i>,
   * accumulator is called with a bag of Sketch Tuples. Unlike <i>exec()</i>, it doesn't serialize the
   * sketch at the end. Instead, it can be called multiple times, each time with another bag of
   * Sketch Tuples to be input to the Intersection.
   *
   * @param inputTuple A tuple containing a single bag, containing Sketch Tuples.
   * @see #exec
   * @see "org.apache.pig.Accumulator.accumulate(org.apache.pig.data.Tuple)"
   * @throws IOException by Pig
   */
  @Override
  public void accumulate(final Tuple inputTuple) throws IOException { //throws is in API
    if (accumIntersection_ == null) {
      accumIntersection_ = SetOperation.builder().setSeed(seed_).buildIntersection();
    }
    final DataBag bag = extractBag(inputTuple);
    if (bag == null) {
      return;
    }

    updateIntersection(bag, accumIntersection_, seed_);
  }

  /**
   * Returns the sketch that has been built up by multiple calls to {@link #accumulate}.
   *
   * @return Sketch Tuple. (see {@link #exec} for return tuple format)
   * @see "org.apache.pig.Accumulator.getValue()"
   */
  @Override
  public Tuple getValue() {
    if ((accumIntersection_ == null) || !accumIntersection_.hasResult()) {
      throw new IllegalStateException(""
          + "The accumulate(Tuple) method must be called at least once with "
          + "a valid inputTuple.bag.SketchTuple prior to calling getValue().");
    }
    final CompactSketch compactSketch = accumIntersection_.getResult(true, null);
    return compactOrderedSketchToTuple(compactSketch);
  }

  /**
   * Cleans up the UDF state after being called using the {@link Accumulator} interface.
   *
   * @see "org.apache.pig.Accumulator.cleanup()"
   */
  @Override
  public void cleanup() {
    accumIntersection_ = null;
  }

  //ALGEBRAIC INTERFACE

  /*************************************************************************************************/
  @Override
  public String getInitial() {
    return Initial.class.getName();
  }

  @Override
  public String getIntermed() {
    return IntermediateFinal.class.getName();
  }

  @Override
  public String getFinal() {
    return IntermediateFinal.class.getName();
  }

  //TOP LEVEL PRIVATE STATIC METHODS

  /*************************************************************************************************
   * Updates an intersection from a bag of sketches
   *
   * @param bag A bag of sketchTuples.
   * @param intersection The intersection to update
   * @param seed to check against incoming sketches
   */
  private static void updateIntersection(final DataBag bag, final Intersection intersection,
      final long seed) {
    //Bag is not empty. process each innerTuple in the bag
    for (Tuple innerTuple : bag) {
      //validate the inner Tuples
      final Object f0 = extractFieldAtIndex(innerTuple, 0);
      if (f0 == null) {
      continue;
    }
      final Byte type = extractTypeAtIndex(innerTuple, 0);
    // add only the first field of the innerTuple to the intersection
    if (type == DataType.BYTEARRAY) {
      final DataByteArray dba = (DataByteArray) f0;
      final Memory srcMem = Memory.wrap(dba.get());
      final Sketch sketch = Sketch.wrap(srcMem, seed);
      intersection.update(sketch);
    }
    else {
      throw new IllegalArgumentException(
          "Field type was not DataType.BYTEARRAY: " + type);
      }
    }
  }

  //STATIC Initial Class only called by Pig

  /*************************************************************************************************
   * Class used to calculate the initial pass of an Algebraic sketch operation.
   *
   * <p>
   * The Initial class simply passes through all records unchanged so that they can be
   * processed by the intermediate processor instead.</p>
   */
  public static class Initial extends EvalFunc<Tuple> {
    //The Algebraic worker classes (Initial, IntermediateFinal) are static and stateless.
    //The constructors and final parameters must mirror the parent class as there is no linkage
    // between them.
    /**
     * Default constructor to make pig validation happy.
     */
    public Initial() {
      this(Long.toString(DEFAULT_UPDATE_SEED));
    }

    /**
     * Constructor for the initial pass of an Algebraic function. Pig will call this and pass the
     * same constructor arguments as the original UDF. In this case the arguments are ignored.
     *
     * @param seedStr <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>.
     */
    public Initial(final String seedStr) {}

    @Override  //Initial exec
    public Tuple exec(final Tuple inputTuple) throws IOException { //throws is in API
      return inputTuple;
    }
  }

  // STATIC IntermediateFinal Class only called by Pig

  /*************************************************************************************************
   * Class used to calculate the intermediate or final combiner pass of an <i>Algebraic</i> intersection
   * operation. This is called from the combiner, and may be called multiple times (from the mapper
   * and from the reducer). It will receive a bag of values returned by either the <i>Intermediate</i>
   * stage or the <i>Initial</i> stages, so it needs to be able to differentiate between and
   * interpret both types.
   */
  public static class IntermediateFinal extends EvalFunc<Tuple> {
    //The Algebraic worker classes (Initial, IntermediateFinal) are static and stateless.
    //The constructors and final parameters must mirror the parent class as there is no linkage
    // between them.
    private final long mySeed_;
    private final Tuple myEmptyCompactOrderedSketchTuple_;

    /**
     * Default constructor to make pig validation happy.  Assumes:
     * <ul>
     * <li><a href="{@docRoot}/resources/dictionary.html#defaultUpdateSeed">See Default Update Seed</a></li>
     * </ul>
     */
    public IntermediateFinal() {
      this(DEFAULT_UPDATE_SEED);
    }

    /**
     * Constructor with strings for the intermediate and final passes of an Algebraic function.
     * Pig will call this and pass the same constructor arguments as the original UDF.
     *
     * @param seedStr <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>.
     */
    public IntermediateFinal(final String seedStr) {
      this(Long.parseLong(seedStr));
    }

    /**
     * Constructor with primitives for the intermediate and final passes of an Algebraic function.
     * Pig will call this and pass the same constructor arguments as the Top Level UDF.
     *
     * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>.
     */
    public IntermediateFinal(final long seed) {
      mySeed_ = seed;
      myEmptyCompactOrderedSketchTuple_ = emptySketchTuple(seed);
    }

    @Override //IntermediateFinal exec
    public Tuple exec(final Tuple inputTuple) throws IOException { //throws is in API

      final Intersection intersection = SetOperation.builder().setSeed(mySeed_).buildIntersection();
      final DataBag outerBag = extractBag(inputTuple); //InputTuple.bag0
      if (outerBag == null) {  //must have non-empty outer bag at field 0.
        return myEmptyCompactOrderedSketchTuple_;
      }
      //Bag is not empty.

      for (Tuple dataTuple : outerBag) {
        final Object f0 = extractFieldAtIndex(dataTuple, 0); //inputTuple.bag0.dataTupleN.f0
        //must have non-null field zero
        if (f0 == null) {
          continue; //go to next dataTuple if there is one.  If none, exception is thrown.
        }
        //f0 is not null
        if (f0 instanceof DataBag) {
          final DataBag innerBag = (DataBag)f0; //inputTuple.bag0.dataTupleN.f0:bag
          if (innerBag.size() == 0) {
            continue; //go to next dataTuple if there is one.  If none, exception is thrown.
          }
          //If field 0 of a dataTuple is again a Bag all tuples of this inner bag
          // will be passed into the union.
          //It is due to system bagged outputs from multiple mapper Initial functions.
          //The Intermediate stage was bypassed.
          updateIntersection(innerBag, intersection, mySeed_); //process all tuples of innerBag

        }
        else if (f0 instanceof DataByteArray) { //inputTuple.bag0.dataTupleN.f0:DBA
          //If field 0 of a dataTuple is a DataByteArray we assume it is a sketch from a prior call
          //It is due to system bagged outputs from multiple mapper Intermediate functions.
          // Each dataTuple.DBA:sketch will merged into the union.
          final DataByteArray dba = (DataByteArray) f0;
          final Memory srcMem = Memory.wrap(dba.get());
          final Sketch sketch = Sketch.wrap(srcMem, mySeed_);
          intersection.update(sketch);
        }
        else { // we should never get here.
          throw new IllegalArgumentException("dataTuple.Field0: Is not a DataByteArray: "
              + f0.getClass().getName());
        }
      }

      final CompactSketch compactSketch = intersection.getResult(true, null);
      return compactOrderedSketchToTuple(compactSketch);
    }

  } //End IntermediateFinal

}
