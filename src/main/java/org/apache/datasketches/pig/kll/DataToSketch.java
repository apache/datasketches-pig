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

package org.apache.datasketches.pig.kll;

import java.io.IOException;

import org.apache.datasketches.kll.KllFloatsSketch;
import org.apache.datasketches.memory.Memory;
import org.apache.pig.Accumulator;
import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

/**
 * This UDF is to build sketches from data.
 * This class implements both the <i>Accumulator</i> and <i>Algebraic</i> interfaces for
 * performance optimization.
 */
public class DataToSketch extends EvalFunc<DataByteArray> implements Accumulator<DataByteArray>, Algebraic {

  private static final TupleFactory TUPLE_FACTORY_ = TupleFactory.getInstance();

  // With the single exception of the Accumulator interface, UDFs are stateless.
  // All parameters kept at the class level must be final, except for the accumSketch.
  private final int k_;
  private KllFloatsSketch accumSketch_;

  // TOP LEVEL API

  /**
   * Default constructor. Assumes default k.
   */
  public DataToSketch() {
    this(KllFloatsSketch.DEFAULT_K);
  }

  /**
   * String constructor.
   *
   * @param kStr string representation of k
   */
  public DataToSketch(final String kStr) {
    this(Integer.parseInt(kStr));
  }

  /**
   * Base constructor.
   *
   * @param k parameter that determines the accuracy and size of the sketch.
   */
  private DataToSketch(final int k) {
    super();
    k_ = k;
  }

  //@formatter:off
  /**
   * Top-level exec function.
   * This method accepts an input Tuple containing a Bag of one or more inner <b>Datum Tuples</b>
   * and returns a single updated <b>Sketch</b> as a DataByteArray.
   *
   * <p>Types are in the form: Java data type: Pig DataType
   *
   * <p><b>Input Tuple</b>
   * <ul>
   *   <li>Tuple: TUPLE (Must contain only one field)
   *     <ul>
   *       <li>index 0: DataBag: BAG (May contain 0 or more Inner Tuples)
   *         <ul>
   *           <li>index 0: Tuple: TUPLE <b>Datum Tuple</b></li>
   *           <li>...</li>
   *           <li>index n-1: Tuple: TUPLE <b>Datum Tuple</b></li>
   *         </ul>
   *       </li>
   *     </ul>
   *   </li>
   * </ul>
   *
   * <b>Datum Tuple</b>
   * <ul>
   *   <li>Tuple: TUPLE (Must contain only one field)
   *     <ul>
   *       <li>index 0: Float: FLOAT</li>
   *     </ul>
   *   </li>
   * </ul>
   *
   * <b>Sketch Tuple</b>
   * <ul>
   *   <li>Tuple: TUPLE (Contains exactly 1 field)
   *     <ul>
   *       <li>index 0: DataByteArray: BYTEARRAY = a serialized KllFloatsSketch object.</li>
   *     </ul>
   *   </li>
   * </ul>
   *
   * @param inputTuple A tuple containing a single bag, containing Datum Tuples
   * @return serialized sketch
   * @see "org.apache.pig.EvalFunc.exec(org.apache.pig.data.Tuple)"
   * @throws IOException from Pig
   */
  // @formatter:on

  @Override // TOP LEVEL EXEC
  public DataByteArray exec(final Tuple inputTuple) throws IOException {
    //The exec is a stateless function. It operates on the input and returns a result.
    final KllFloatsSketch sketch = new KllFloatsSketch(k_);
    if ((inputTuple != null) && (inputTuple.size() > 0)) {
      final DataBag bag = (DataBag) inputTuple.get(0);
      for (final Tuple innerTuple: bag) {
        sketch.update((Float) innerTuple.get(0));
      }
    }
    return new DataByteArray(sketch.toByteArray());
  }

  // ACCUMULATOR INTERFACE

  /**
   * An <i>Accumulator</i> version of the standard <i>exec()</i> method. Like <i>exec()</i>,
   * accumulator is called with a bag of Datum Tuples. Unlike <i>exec()</i>, it doesn't serialize the
   * sketch at the end. Instead, it can be called multiple times, each time with another bag of Datum Tuples.
   *
   * @param inputTuple A tuple containing a single bag, containing Datum Tuples.
   * @see #exec
   * @see "org.apache.pig.Accumulator.accumulate(org.apache.pig.data.Tuple)"
   * @throws IOException by Pig
   */
  @Override
  public void accumulate(final Tuple inputTuple) throws IOException {
    if ((inputTuple == null) || (inputTuple.size() == 0)) { return; }
    final DataBag bag = (DataBag) inputTuple.get(0);
    if (bag == null) { return; }
    if (accumSketch_ == null) {
      accumSketch_ = new KllFloatsSketch(k_);
    }
    for (final Tuple innerTuple: bag) {
      accumSketch_.update((Float) innerTuple.get(0));
    }
  }

  /**
   * Returns the result that has been built up by multiple calls to {@link #accumulate}.
   *
   * @return serialized sketch
   * @see "org.apache.pig.Accumulator.getValue()"
   */
  @Override
  public DataByteArray getValue() {
    if (accumSketch_ != null) {
      return new DataByteArray(accumSketch_.toByteArray());
    }
    // return empty sketch
    return new DataByteArray(new KllFloatsSketch(k_).toByteArray());
  }

  /**
   * Cleans up the UDF state after being called using the {@link Accumulator} interface.
   *
   * @see "org.apache.pig.Accumulator.cleanup()"
   */
  @Override
  public void cleanup() {
    accumSketch_ = null;
  }

  // ALGEBRAIC INTERFACE

  @Override
  public String getInitial() {
    return Initial.class.getName();
  }

  @Override
  public String getIntermed() {
    return Intermediate.class.getName();
  }

  @Override
  public String getFinal() {
    return Final.class.getName();
  }

  // STATIC Initial Class only called by Pig

  /**
   * Class used to calculate the initial pass of an Algebraic sketch operation.
   *
   * <p>
   * The Initial class simply passes through all records unchanged so that they can be
   * processed by the intermediate processor instead.</p>
   */
  public static class Initial extends EvalFunc<Tuple> {
    // The Algebraic worker classes (Initial, Intermediate and Final) are static and stateless.
    // The constructors and parameters must mirror the parent class as there is no linkage
    // between them.
    /**
     * Default constructor.
     */
    public Initial() {}

    /**
     * Constructor with explicit k as string.
     *
     * @param kStr string representation of k
     */
    public Initial(final String kStr) {}

    @Override
    public Tuple exec(final Tuple inputTuple) throws IOException {
      return inputTuple;
    }
  }

  // STATIC Intermediate Class only called by Pig

  /**
   * Class used to calculate the intermediate pass of an <i>Algebraic</i> sketch operation.
   * It will receive a bag of values returned by either the <i>Intermediate</i>
   * stage or the <i>Initial</i> stages, so it needs to be able to differentiate between and
   * interpret both types.
   */
  public static class Intermediate extends EvalFunc<Tuple> {
    // The Algebraic worker classes (Initial, Intermediate and Final) are static and stateless.
    // The constructors and parameters must mirror the parent class as there is no linkage
    // between them.
    private final int k_;

    /**
     * Default constructor. Assumes default k.
     */
    public Intermediate() {
      this(KllFloatsSketch.DEFAULT_K);
    }

    /**
     * Constructor with explicit k as string. Pig will call.
     * this and pass the same constructor arguments as the base UDF.
     *
     * @param kStr string representation of k
     */
    public Intermediate(final String kStr) {
      this(Integer.parseInt(kStr));
    }

    /**
     * Constructor with primitive k.
     *
     * @param k parameter that determines the accuracy and size of the sketch.
     */
    private Intermediate(final int k) {
      k_ = k;
    }

    @Override
    public Tuple exec(final Tuple inputTuple) throws IOException { //throws is in API
      return TUPLE_FACTORY_.newTuple(process(inputTuple, k_));
    }
  }

  // STATIC Final Class only called by Pig

  /**
   * Class used to calculate the final pass of an <i>Algebraic</i> sketch operation.
   * It will receive a bag of values returned by either the <i>Intermediate</i>
   * stage or the <i>Initial</i> stages, so it needs to be able to differentiate between and
   * interpret both types.
   */
  public static class Final extends EvalFunc<DataByteArray> {
    // The Algebraic worker classes (Initial, Intermediate and Final) are static and stateless.
    // The constructors and parameters must mirror the parent class as there is no linkage
    // between them.
    private final int k_;

    /**
     * Default constructor. Assumes default k.
     */
    public Final() {
      this(KllFloatsSketch.DEFAULT_K);
    }

    /**
     * Constructor with explicit k as string. Pig will call
     * this and pass the same constructor arguments as the base UDF.
     *
     * @param kStr string representation of k
     */
    public Final(final String kStr) {
      this(Integer.parseInt(kStr));
    }

    /**
     * Constructor with primitive k.
     *
     * @param k parameter that determines the accuracy and size of the sketch.
     */
    private Final(final int k) {
      k_ = k;
    }

    @Override
    public DataByteArray exec(final Tuple inputTuple) throws IOException {
      return process(inputTuple, k_);
    }
  }

  private static DataByteArray process(final Tuple inputTuple, final int k) throws IOException {
    final KllFloatsSketch sketch = new KllFloatsSketch(k);
    if ((inputTuple != null) && (inputTuple.size() > 0)) {
      final DataBag outerBag = (DataBag) inputTuple.get(0);
      for (final Tuple dataTuple: outerBag) {
        final Object f0 = dataTuple.get(0);
        if (f0 == null) { continue; }
        if (f0 instanceof DataBag) {
          final DataBag innerBag = (DataBag) f0; // inputTuple.bag0.dataTupleN.f0:bag
          if (innerBag.size() == 0) { continue; }
          // If field 0 of a dataTuple is a Bag all innerTuples of this inner bag
          // will be passed into the union.
          // It is due to system bagged outputs from multiple mapper Initial functions.
          // The Intermediate stage was bypassed.
          for (final Tuple innerTuple: innerBag) {
            sketch.update((Float) innerTuple.get(0));
          }
        } else if (f0 instanceof DataByteArray) { // inputTuple.bag0.dataTupleN.f0:DBA
          // If field 0 of a dataTuple is a DataByteArray we assume it is a sketch
          // due to system bagged outputs from multiple mapper Intermediate functions.
          // Each dataTuple.DBA:sketch will merged into the union.
          final DataByteArray dba = (DataByteArray) f0;
          sketch.merge(KllFloatsSketch.heapify(Memory.wrap(dba.get())));
        } else {
          throw new IllegalArgumentException("dataTuple.Field0: Is not a DataByteArray: "
              + f0.getClass().getName());
        }
      }
    }
    return new DataByteArray(sketch.toByteArray());
  }

}
