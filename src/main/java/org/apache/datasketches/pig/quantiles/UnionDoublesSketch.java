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

import java.io.IOException;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.quantiles.DoublesSketch;
import org.apache.datasketches.quantiles.DoublesUnion;
import org.apache.datasketches.quantiles.DoublesUnionBuilder;
import org.apache.pig.Accumulator;
import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * This is a Pig UDF that merges Quantiles Sketches.
 * To assist Pig, this class implements both the <i>Accumulator</i> and <i>Algebraic</i> interfaces.
 */
public class UnionDoublesSketch extends EvalFunc<Tuple> implements Accumulator<Tuple>, Algebraic {

  private static final TupleFactory tupleFactory_ = TupleFactory.getInstance();

  // With the single exception of the Accumulator interface, UDFs are stateless.
  // All parameters kept at the class level must be final, except for the accumUnion.
  private final DoublesUnionBuilder unionBuilder_;
  private DoublesUnion accumUnion_;

  //TOP LEVEL API

  /**
   * Default constructor. Assumes default k.
   */
  public UnionDoublesSketch() {
    this(0);
  }

  /**
   * String constructor.
   *
   * @param kStr string representation of k
   */
  public UnionDoublesSketch(final String kStr) {
    this(Integer.parseInt(kStr));
  }

  /**
   * Base constructor.
   *
   * @param k parameter that determines the accuracy and size of the sketch.
   */
  public UnionDoublesSketch(final int k) {
    super();
    unionBuilder_ = DoublesUnion.builder();
    if (k > 0) {
      unionBuilder_.setMaxK(k);
    }
  }

  //@formatter:off
  /**
   * Top-level exec function.
   * This method accepts an input Tuple containing a Bag of one or more inner <b>Sketch Tuples</b>
   * and returns a single updated <b>Sketch</b> as a <b>Sketch Tuple</b>.
   *
   * <p>If a large number of calls are anticipated, leveraging either the <i>Algebraic</i> or
   * <i>Accumulator</i> interfaces is recommended. Pig normally handles this automatically.
   *
   * <p>Internally, this method presents the inner <b>Sketch Tuples</b> to a new <b>Union</b>.
   * The result is returned as a <b>Sketch Tuple</b>
   *
   * <p>Types are in the form: Java data type: Pig DataType
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
   * @return Sketch Tuple. If inputTuple is null or empty, returns empty sketch.
   * @see "org.apache.pig.EvalFunc.exec(org.apache.pig.data.Tuple)"
   */
  //@formatter:on

  @Override // TOP LEVEL EXEC
  public Tuple exec(final Tuple inputTuple) throws IOException {
    //The exec is a stateless function.  It operates on the input and returns a result.
    if (inputTuple != null && inputTuple.size() > 0) {
      final DoublesUnion union = unionBuilder_.build();
      final DataBag bag = (DataBag) inputTuple.get(0);
      updateUnion(bag, union);
      final DoublesSketch resultSketch = union.getResultAndReset();
      if (resultSketch != null) {
        return tupleFactory_.newTuple(new DataByteArray(resultSketch.toByteArray(true)));
      }
    }
    // return empty sketch
    return tupleFactory_.newTuple(new DataByteArray(unionBuilder_.build().getResult().toByteArray(true)));
  }

  @Override
  public Schema outputSchema(final Schema input) {
    if (input == null) { return null; }
    try {
      final Schema tupleSchema = new Schema();
      tupleSchema.add(new Schema.FieldSchema("Sketch", DataType.BYTEARRAY));
      return new Schema(new Schema.FieldSchema(getSchemaName(
          this.getClass().getName().toLowerCase(), input), tupleSchema, DataType.TUPLE));
    } catch (final FrontendException e) {
      throw new RuntimeException(e);
    }
  }

  //ACCUMULATOR INTERFACE

  /**
   * An <i>Accumulator</i> version of the standard <i>exec()</i> method. Like <i>exec()</i>,
   * accumulator is called with a bag of Sketch Tuples. Unlike <i>exec()</i>, it doesn't serialize the
   * sketch at the end. Instead, it can be called multiple times, each time with another bag of
   * Sketch Tuples to be input to the Union.
   *
   * @param inputTuple A tuple containing a single bag, containing Sketch Tuples.
   * @see #exec
   * @see "org.apache.pig.Accumulator.accumulate(org.apache.pig.data.Tuple)"
   * @throws IOException by Pig
   */
  @Override
  public void accumulate(final Tuple inputTuple) throws IOException {
    if (inputTuple == null || inputTuple.size() == 0) { return; }
    final DataBag bag = (DataBag) inputTuple.get(0);
    if (bag == null) { return; }
    if (accumUnion_ == null) {
      accumUnion_ = unionBuilder_.build();
    }
    updateUnion(bag, accumUnion_);
  }

  /**
   * Returns the result of the Union that has been built up by multiple calls to {@link #accumulate}.
   *
   * @return Sketch Tuple. (see {@link #exec} for return tuple format)
   * @see "org.apache.pig.Accumulator.getValue()"
   */
  @Override
  public Tuple getValue() {
    if (accumUnion_ != null) {
      final DoublesSketch resultSketch = accumUnion_.getResultAndReset();
      if (resultSketch != null) {
        return tupleFactory_.newTuple(new DataByteArray(resultSketch.toByteArray(true)));
      }
    }
    // return empty sketch
    return tupleFactory_.newTuple(new DataByteArray(unionBuilder_.build().getResult().toByteArray(true)));
  }

  /**
   * Cleans up the UDF state after being called using the {@link Accumulator} interface.
   *
   * @see "org.apache.pig.Accumulator.cleanup()"
   */
  @Override
  public void cleanup() {
    accumUnion_ = null;
  }

  //ALGEBRAIC INTERFACE

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

  /**
   * Updates a union from a bag of sketches
   *
   * @param bag A bag of sketchTuples.
   * @param union The union to update
   */
  private static void updateUnion(final DataBag bag, final DoublesUnion union) throws ExecException {
    for (Tuple innerTuple: bag) {
      final Object f0 = innerTuple.get(0);
      if (f0 == null) { continue; }
      if (f0 instanceof DataByteArray) {
        final DataByteArray dba = (DataByteArray) f0;
        if (dba.size() > 0) {
          union.update(Memory.wrap(dba.get()));
        }
      } else {
        throw new IllegalArgumentException("Field type was not DataType.BYTEARRAY: " + innerTuple.getType(0));
      }
    }
  }

  //STATIC Initial Class only called by Pig

  /**
   * Class used to calculate the initial pass of an Algebraic sketch operation.
   *
   * <p>
   * The Initial class simply passes through all records unchanged so that they can be
   * processed by the intermediate processor instead.</p>
   */
  public static class Initial extends EvalFunc<Tuple> {
    // The Algebraic worker classes (Initial, IntermediateFinal) are static and stateless.
    // The constructors and final parameters must mirror the parent class as there is no linkage
    // between them.
    /**
     * Default constructor.
     */
    public Initial() {}

    /**
     * Constructor for the initial pass of an Algebraic function. Pig will call this and pass the
     * same constructor arguments as the base UDF. In this case the arguments are ignored.
     *
     * @param kStr string representation of k
     */
    public Initial(final String kStr) {}

    @Override // Initial exec
    public Tuple exec(final Tuple inputTuple) throws IOException {
      return inputTuple;
    }
  }

  // STATIC IntermediateFinal Class only called by Pig

  /**
   * Class used to calculate the intermediate or final combiner pass of an <i>Algebraic</i> union
   * operation. This is called from the combiner, and may be called multiple times (from the mapper
   * and from the reducer). It will receive a bag of values returned by either the <i>Intermediate</i>
   * stage or the <i>Initial</i> stages, so it needs to be able to differentiate between and
   * interpret both types.
   */
  public static class IntermediateFinal extends EvalFunc<Tuple> {
    // The Algebraic worker classes (Initial, IntermediateFinal) are static and stateless.
    // The constructors and final parameters must mirror the parent class as there is no linkage
    // between them.
    private final DoublesUnionBuilder unionBuilder_;

    /**
     * Default constructor. Assumes default k.
     */
    public IntermediateFinal() {
      this(0);
    }

    /**
     * Constructor for the intermediate and final passes of an Algebraic function. Pig will call
     * this and pass the same constructor arguments as the base UDF.
     *
     * @param kStr string representation of k
     */
    public IntermediateFinal(final String kStr) {
      this(Integer.parseInt(kStr));
    }

    /**
     * Constructor with primitives for the intermediate and final passes of an Algebraic function.
     *
     * @param k parameter that determines the accuracy and size of the sketch.
     */
    public IntermediateFinal(final int k) {
      unionBuilder_ = DoublesUnion.builder();
      if (k > 0) { unionBuilder_.setMaxK(k); }
    }

    @Override // IntermediateFinal exec
    public Tuple exec(final Tuple inputTuple) throws IOException {
      if (inputTuple != null && inputTuple.size() > 0) {
        final DoublesUnion union = unionBuilder_.build();
        final DataBag outerBag = (DataBag) inputTuple.get(0);
        for (final Tuple dataTuple: outerBag) {
          final Object f0 = dataTuple.get(0);
          if (f0 == null) { continue; }
          if (f0 instanceof DataBag) {
            final DataBag innerBag = (DataBag) f0; //inputTuple.bag0.dataTupleN.f0:bag
            if (innerBag.size() == 0) { continue; }
            // If field 0 of a dataTuple is again a Bag all tuples of this inner bag
            // will be passed into the union.
            // It is due to system bagged outputs from multiple mapper Initial functions.
            // The Intermediate stage was bypassed.
            updateUnion(innerBag, union);
          } else if (f0 instanceof DataByteArray) { //inputTuple.bag0.dataTupleN.f0:DBA
            // If field 0 of a dataTuple is a DataByteArray we assume it is a sketch from a prior call
            // It is due to system bagged outputs from multiple mapper Intermediate functions.
            // Each dataTuple.DBA:sketch will merged into the union.
            final DataByteArray dba = (DataByteArray) f0;
            union.update(Memory.wrap(dba.get()));
          } else {
            throw new IllegalArgumentException("dataTuple.Field0: Is not a DataByteArray: "
              + f0.getClass().getName());
          }
        }
        final DoublesSketch resultSketch = union.getResultAndReset();
        if (resultSketch != null) {
          return tupleFactory_.newTuple(new DataByteArray(resultSketch.toByteArray(true)));
        }
      }
      // return empty sketch
      return tupleFactory_.newTuple(new DataByteArray(unionBuilder_.build().getResult().toByteArray(true)));
    }
  } // end IntermediateFinal

}
