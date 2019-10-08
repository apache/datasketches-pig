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
import java.util.Comparator;

import org.apache.datasketches.ArrayOfItemsSerDe;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.quantiles.ItemsSketch;
import org.apache.datasketches.quantiles.ItemsUnion;
import org.apache.pig.Accumulator;
import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * Builds ItemsSketch from data.
 * To assist Pig, this class implements both the <i>Accumulator</i> and <i>Algebraic</i> interfaces.
 * @param <T> type of item
 */
public abstract class DataToItemsSketch<T> extends EvalFunc<Tuple>
    implements Accumulator<Tuple>, Algebraic {

  private static final TupleFactory tupleFactory_ = TupleFactory.getInstance();

  // With the single exception of the Accumulator interface, UDFs are stateless.
  // All parameters kept at the class level must be final, except for the accumUnion.
  private final int k_;
  private final Comparator<T> comparator_;
  private final ArrayOfItemsSerDe<T> serDe_;
  private ItemsUnion<T> accumUnion_;

  // TOP LEVEL API

  /**
   * Base constructor.
   *
   * @param k parameter that determines the accuracy and size of the sketch.
   * The value of 0 means the default k, whatever it is in the sketches-core library
   * @param comparator for items of type T
   * @param serDe an instance of ArrayOfItemsSerDe for type T
   */
  public DataToItemsSketch(final int k, final Comparator<T> comparator,
      final ArrayOfItemsSerDe<T> serDe) {
    super();
    k_ = k;
    comparator_ = comparator;
    serDe_ = serDe;
  }

  //@formatter:off
  /**
   * Top-level exec function.
   * This method accepts an input Tuple containing a Bag of one or more inner <b>Datum Tuples</b>
   * and returns a single <b>Sketch</b> as a <b>Sketch Tuple</b>.
   *
   * <p>If a large number of calls is anticipated, leveraging either the <i>Algebraic</i> or
   * <i>Accumulator</i> interfaces is recommended. Pig normally handles this automatically.
   *
   * <p>Internally, this method presents the inner <b>Datum Tuples</b> to a new <b>Union</b>,
   * which is returned as a <b>Sketch Tuple</b>
   *
   * <p>Types below are in the form: Java data type: Pig DataType
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
   *       <li>index 0: T: some suitable Pig type convertible to T</li>
   *     </ul>
   *   </li>
   * </ul>
   *
   * <b>Sketch Tuple</b>
   * <ul>
   *   <li>Tuple: TUPLE (Contains exactly 1 field)
   *     <ul>
   *       <li>index 0: DataByteArray: BYTEARRAY = a serialized QuantilesSketch object.</li>
   *     </ul>
   *   </li>
   * </ul>
   *
   * @param inputTuple A tuple containing a single bag, containing Datum Tuples.
   * @return Sketch Tuple. If inputTuple is null or empty, returns empty sketch.
   * @see "org.apache.pig.EvalFunc.exec(org.apache.pig.data.Tuple)"
   * @throws IOException from Pig.
   */
  // @formatter:on

  @Override // TOP LEVEL EXEC
  public Tuple exec(final Tuple inputTuple) throws IOException {
    //The exec is a stateless function. It operates on the input and returns a result.
    if ((inputTuple != null) && (inputTuple.size() > 0)) {
      final ItemsUnion<T> union = k_ > 0
          ? ItemsUnion.getInstance(k_, comparator_)
          : ItemsUnion.getInstance(comparator_);
      final DataBag bag = (DataBag) inputTuple.get(0);
      for (final Tuple innerTuple: bag) {
        final Object value = innerTuple.get(0);
        if (value != null) {
          union.update(extractValue(value));
        }
      }
      final ItemsSketch<T> resultSketch = union.getResultAndReset();
      if (resultSketch != null) {
        return tupleFactory_.newTuple(new DataByteArray(resultSketch.toByteArray(serDe_)));
      }
    }
    // return empty sketch
    final ItemsSketch<T> sketch = k_ > 0
        ? ItemsSketch.getInstance(k_, comparator_)
        : ItemsSketch.getInstance(comparator_);
    return tupleFactory_.newTuple(new DataByteArray(sketch.toByteArray(serDe_)));
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

  // ACCUMULATOR INTERFACE

  /**
   * An <i>Accumulator</i> version of the standard <i>exec()</i> method. Like <i>exec()</i>,
   * accumulator is called with a bag of Datum Tuples. Unlike <i>exec()</i>, it doesn't serialize the
   * sketch at the end. Instead, it can be called multiple times, each time with another bag of
   * Datum Tuples to be input to the Union.
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
    if (accumUnion_ == null) {
      accumUnion_ = k_ > 0
        ? ItemsUnion.getInstance(k_, comparator_)
        : ItemsUnion.getInstance(comparator_);
    }
    for (final Tuple innerTuple: bag) {
      final Object value = innerTuple.get(0);
      if (value != null) {
        accumUnion_.update(extractValue(value));
      }
    }
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
      final ItemsSketch<T> resultSketch = accumUnion_.getResultAndReset();
      if (resultSketch != null) {
        return tupleFactory_.newTuple(new DataByteArray(resultSketch.toByteArray(serDe_)));
      }
    }
    // return empty sketch
    final ItemsSketch<T> sketch = k_ > 0
        ? ItemsSketch.getInstance(k_, comparator_)
        : ItemsSketch.getInstance(comparator_);
    return tupleFactory_.newTuple(new DataByteArray(sketch.toByteArray(serDe_)));
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

  /**
   * Override this if it takes more than a cast to convert from Pig type to type T
   * @param object Pig object, which needs to be converted to type T
   * @return value of type T
   */
  @SuppressWarnings("unchecked")
  protected T extractValue(final Object object) {
    return (T) object;
  }

  // STATIC Initial Class only called by Pig

  /**
   * Class used to calculate the initial pass of an Algebraic sketch operation.
   *
   * <p>
   * The Initial class simply passes through all records unchanged so that they can be
   * processed by the intermediate processor instead.</p>
   */
  public static class DataToItemsSketchInitial extends EvalFunc<Tuple> {
    // The Algebraic worker classes (Initial, IntermediateFinal) are static and stateless.
    // The constructors must mirror the main UDF class
    /**
     * Default constructor.
     */
    public DataToItemsSketchInitial() {}

    /**
     * Constructor with specific k
     * @param kStr string representation of k
     */
    public DataToItemsSketchInitial(final String kStr) {}

    @Override
    public Tuple exec(final Tuple inputTuple) throws IOException {
      return inputTuple;
    }
  }

  // STATIC IntermediateFinal Class only called by Pig

  /**
   * Class used to calculate the intermediate or final combiner pass of an <i>Algebraic</i> sketch
   * operation. This is called from the combiner, and may be called multiple times (from the mapper
   * and from the reducer). It will receive a bag of values returned by either the <i>Intermediate</i>
   * stage or the <i>Initial</i> stages, so it needs to be able to differentiate between and
   * interpret both types.
   * @param <T> type of item
   */
  public static abstract class DataToItemsSketchIntermediateFinal<T> extends EvalFunc<Tuple> {
    // The Algebraic worker classes (Initial, IntermediateFinal) are static and stateless.
    // The constructors of the concrete class must mirror the ones in the main UDF class
    private final int k_;
    private final Comparator<T> comparator_;
    private final ArrayOfItemsSerDe<T> serDe_;

    /**
     * Constructor for the intermediate and final passes of an Algebraic function.
     *
     * @param k parameter that determines the accuracy and size of the sketch.
     * @param comparator for items of type T
     * @param serDe an instance of ArrayOfItemsSerDe for type T
     */
    public DataToItemsSketchIntermediateFinal(
        final int k, final Comparator<T> comparator, final ArrayOfItemsSerDe<T> serDe) {
      super();
      k_ = k;
      comparator_ = comparator;
      serDe_ = serDe;
    }

    /**
     * Override this if it takes more than a cast to convert from Pig type to type T
     * @param object Pig object, which needs to be converted to type T
     * @return value of type T
     */
    @SuppressWarnings("unchecked")
    protected T extractValue(final Object object) {
      return (T) object;
    }

    @Override // IntermediateFinal exec
    public Tuple exec(final Tuple inputTuple) throws IOException { //throws is in API
      if ((inputTuple != null) && (inputTuple.size() > 0)) {
        final ItemsUnion<T> union = k_ > 0
            ? ItemsUnion.getInstance(k_, comparator_)
            : ItemsUnion.getInstance(comparator_);
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
              final Object value = innerTuple.get(0);
              if (value != null) {
                union.update(extractValue(value));
              }
            }
          } else if (f0 instanceof DataByteArray) { // inputTuple.bag0.dataTupleN.f0:DBA
            // If field 0 of a dataTuple is a DataByteArray we assume it is a sketch
            // due to system bagged outputs from multiple mapper Intermediate functions.
            // Each dataTuple.DBA:sketch will merged into the union.
            final DataByteArray dba = (DataByteArray) f0;
            union.update(ItemsSketch.getInstance(Memory.wrap(dba.get()), comparator_, serDe_));
          } else {
            throw new IllegalArgumentException("dataTuple.Field0: Is not a DataByteArray: "
                + f0.getClass().getName());
          }
        }
        final ItemsSketch<T> resultSketch = union.getResultAndReset();
        if (resultSketch != null) {
          return tupleFactory_.newTuple(new DataByteArray(resultSketch.toByteArray(serDe_)));
        }
      }
      // return empty sketch
      final ItemsSketch<T> sketch = k_ > 0
          ? ItemsSketch.getInstance(k_, comparator_)
          : ItemsSketch.getInstance(comparator_);
      return tupleFactory_.newTuple(new DataByteArray(sketch.toByteArray(serDe_)));
    }
  } // end IntermediateFinal

}
