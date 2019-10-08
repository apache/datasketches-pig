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

import static org.apache.datasketches.Util.DEFAULT_NOMINAL_ENTRIES;
import static org.apache.datasketches.Util.DEFAULT_UPDATE_SEED;
import static org.apache.datasketches.Util.checkIfPowerOf2;
import static org.apache.datasketches.Util.checkProbability;
import static org.apache.datasketches.pig.theta.PigUtil.RF;
import static org.apache.datasketches.pig.theta.PigUtil.compactOrderedSketchToTuple;
import static org.apache.datasketches.pig.theta.PigUtil.emptySketchTuple;
import static org.apache.datasketches.pig.theta.PigUtil.extractBag;
import static org.apache.datasketches.pig.theta.PigUtil.extractFieldAtIndex;
import static org.apache.datasketches.pig.theta.PigUtil.extractTypeAtIndex;

import java.io.IOException;

import org.apache.datasketches.Util;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.CompactSketch;
import org.apache.datasketches.theta.SetOperation;
import org.apache.datasketches.theta.Union;
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
 * This is a Pig UDF that builds Sketches from data.
 * To assist Pig, this class implements both the <i>Accumulator</i> and <i>Algebraic</i> interfaces.
 */
public class DataToSketch extends EvalFunc<Tuple> implements Accumulator<Tuple>, Algebraic {
  //With the single exception of the Accumulator interface, UDFs are stateless.
  //All parameters kept at the class level must be final, except for the accumUnion.
  private final int nomEntries_;
  private final float p_;
  private final long seed_;
  private final Tuple emptyCompactOrderedSketchTuple_;
  private Union accumUnion_;

  //TOP LEVEL API

  /**
   * Default constructor. Assumes:
   * <ul>
   * <li><a href="{@docRoot}/resources/dictionary.html#defaultNomEntries">See Default Nominal Entries</a></li>
   * <li><i>p</i> = 1.0. <a href="{@docRoot}/resources/dictionary.html#p">See Sampling Probability,
   * <i>p</i></a>.</li>
   * <li><a href="{@docRoot}/resources/dictionary.html#defaultUpdateSeed">See Default Update Seed</a></li>
   * </ul>
   */
  public DataToSketch() {
    this(DEFAULT_NOMINAL_ENTRIES, (float)(1.0), DEFAULT_UPDATE_SEED);
  }

  /**
   * String constructor. Assumes:
   * <ul>
   * <li><i>p</i> = 1.0. <a href="{@docRoot}/resources/dictionary.html#p">See Sampling Probability,
   * <i>p</i></a></li>
   * <li><a href="{@docRoot}/resources/dictionary.html#defaultUpdateSeed">See Default Update Seed</a></li>
   * </ul>
   *
   * @param nomEntriesStr <a href="{@docRoot}/resources/dictionary.html#nomEntries">See Nominal Entries</a>
   */
  public DataToSketch(final String nomEntriesStr) {
    this(Integer.parseInt(nomEntriesStr), (float)(1.0), DEFAULT_UPDATE_SEED);
  }

  /**
   * String constructor. Assumes:
   * <ul>
   * <li><a href="{@docRoot}/resources/dictionary.html#defaultUpdateSeed">See Default Update Seed</a></li>
   * </ul>
   *
   * @param nomEntriesStr <a href="{@docRoot}/resources/dictionary.html#nomEntries">See Nominal Entries</a>
   * @param pStr <a href="{@docRoot}/resources/dictionary.html#p">See Sampling Probability, <i>p</i></a>
   */
  public DataToSketch(final String nomEntriesStr, final String pStr) {
    this(Integer.parseInt(nomEntriesStr), Float.parseFloat(pStr), DEFAULT_UPDATE_SEED);
  }

  /**
   * Full string constructor.
   *
   * @param nomEntriesStr <a href="{@docRoot}/resources/dictionary.html#nomEntries">See Nominal Entries</a>.
   * @param pStr <a href="{@docRoot}/resources/dictionary.html#p">See Sampling Probability, <i>p</i></a>.
   * @param seedStr  <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>.
   */
  public DataToSketch(final String nomEntriesStr, final String pStr, final String seedStr) {
    this(Integer.parseInt(nomEntriesStr), Float.parseFloat(pStr), Long.parseLong(seedStr));
  }

  /**
   * Base constructor.
   *
   * @param nomEntries <a href="{@docRoot}/resources/dictionary.html#nomEntries">See Nominal Entries</a>.
   * @param p <a href="{@docRoot}/resources/dictionary.html#p">See Sampling Probability, <i>p</i></a>.
   * @param seed  <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>.
   */
  public DataToSketch(final int nomEntries, final float p, final long seed) {
    super();
    nomEntries_ = nomEntries;
    p_ = p;
    seed_ = seed;
    emptyCompactOrderedSketchTuple_ = emptySketchTuple(seed);
    //Catch these errors during construction, don't wait for the exec to be called.
    checkIfPowerOf2(nomEntries, "nomEntries");
    checkProbability(p, "p");
    if (nomEntries < (1 << Util.MIN_LG_NOM_LONGS)) {
      throw new IllegalArgumentException("NomEntries too small: " + nomEntries
          + ", required: " + (1 << Util.MIN_LG_NOM_LONGS));
    }
  }

  //@formatter:off
  /*************************************************************************************************
   * Top-level exec function.
   * This method accepts an input Tuple containing a Bag of one or more inner <b>Datum Tuples</b>
   * and returns a single updated <b>Sketch</b> as a <b>Sketch Tuple</b>.
   *
   * <p>If a large number of calls is anticipated, leveraging either the <i>Algebraic</i> or
   * <i>Accumulator</i> interfaces is recommended. Pig normally handles this automatically.
   *
   * <p>Internally, this method presents the inner <b>Datum Tuples</b> to a new <b>Sketch</b>,
   * which is returned as a <b>Sketch Tuple</b>
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
   *       <li>index 0: Java data type : Pig DataType: may be any one of:
   *         <ul>
   *           <li>Byte: BYTE</li>
   *           <li>Integer: INTEGER</li>
   *           <li>Long: LONG</li>
   *           <li>Float: FLOAT</li>
   *           <li>Double: DOUBLE</li>
   *           <li>String: CHARARRAY</li>
   *           <li>DataByteArray: BYTEARRAY</li>
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
   * @param inputTuple A tuple containing a single bag, containing Datum Tuples.
   * @return Sketch Tuple. If inputTuple is null or empty, returns empty sketch (8 bytes).
   * @see "org.apache.pig.EvalFunc.exec(org.apache.pig.data.Tuple)"
   * @throws IOException from Pig.
   */
  // @formatter:on

  @Override //TOP LEVEL EXEC
  public Tuple exec(final Tuple inputTuple) throws IOException { //throws is in API
    //The exec is a stateless function.  It operates on the input and returns a result.
    // It can only call static functions.
    final Union union = newUnion(nomEntries_, p_, seed_);
    final DataBag bag = extractBag(inputTuple);
    if (bag == null) {
      return emptyCompactOrderedSketchTuple_; //Configured with parent
    }

    updateUnion(bag, union); //updates union with all elements of the bag
    final CompactSketch compOrdSketch = union.getResult(true, null);
    return compactOrderedSketchToTuple(compOrdSketch);
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
   * accumulator is called with a bag of Datum Tuples. Unlike <i>exec()</i>, it doesn't serialize the
   * sketch at the end. Instead, it can be called multiple times, each time with another bag of
   * Datum Tuples to be input to the sketch.
   *
   * @param inputTuple A tuple containing a single bag, containing Datum Tuples.
   * @see #exec
   * @see "org.apache.pig.Accumulator.accumulate(org.apache.pig.data.Tuple)"
   * @throws IOException by Pig
   */
  @Override
  public void accumulate(final Tuple inputTuple) throws IOException { //throws is in API
    if (accumUnion_ == null) {
      accumUnion_ = DataToSketch.newUnion(nomEntries_, p_, seed_);
    }
    final DataBag bag = extractBag(inputTuple);
    if (bag == null) { return; }

    updateUnion(bag, accumUnion_);
  }

  /**
   * Returns the sketch that has been built up by multiple calls to {@link #accumulate}.
   *
   * @return Sketch Tuple. (see {@link #exec} for return tuple format)
   * @see "org.apache.pig.Accumulator.getValue()"
   */
  @Override
  public Tuple getValue() {
    if (accumUnion_ == null) {
      return emptyCompactOrderedSketchTuple_; //Configured with parent
    }
    final CompactSketch compOrdSketch = accumUnion_.getResult(true, null);
    return compactOrderedSketchToTuple(compOrdSketch);
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

  /**
   * Return a new empty HeapUnion
   * @param nomEntries the given nominal entries
   * @param p the given probability p
   * @param seed the given seed
   * @return a new empty HeapUnion
   */
  private static final Union newUnion(final int nomEntries, final float p, final long seed) {
    return SetOperation.builder()
        .setSeed(seed).setP(p).setResizeFactor(RF).setNominalEntries(nomEntries).buildUnion();
  }

  /*************************************************************************************************
   * Updates a union with the data from the given bag.
   *
   * @param bag A bag of tuples to insert.
   * @param union the union to update
   */
  private static void updateUnion(final DataBag bag, final Union union) {
    //Bag is not empty. process each innerTuple in the bag
    for (Tuple innerTuple : bag) {
      final Object f0 = extractFieldAtIndex(innerTuple, 0); //consider only field 0
      if (f0 == null) {
        continue;
      }
      final Byte type = extractTypeAtIndex(innerTuple, 0);
      if (type == null) {
        continue;
      }

      switch (type) {
        case DataType.NULL:
          break;
        case DataType.BYTE:
          union.update((byte) f0);
          break;
        case DataType.INTEGER:
          union.update((int) f0);
          break;
        case DataType.LONG:
          union.update((long) f0);
          break;
        case DataType.FLOAT:
          union.update((float) f0);
          break;
        case DataType.DOUBLE:
          union.update((double) f0);
          break;
        case DataType.BYTEARRAY: {
          final DataByteArray dba = (DataByteArray) f0;
          union.update(dba.get()); //checks null, empty
          break;
        }
        case DataType.CHARARRAY: {
          union.update(f0.toString()); //checks null, empty
          break;
        }
        default: // types not handled
          throw new IllegalArgumentException("Field 0 of innerTuple must be one of "
              + "NULL, BYTE, INTEGER, LONG, FLOAT, DOUBLE, BYTEARRAY or CHARARRAY. "
              + "Given Type = " + DataType.findTypeName(type)
              + ", Object = " + f0.toString());
      } //End switch
    } //End for
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
      this(Integer.toString(Util.DEFAULT_NOMINAL_ENTRIES), "1.0",
          Long.toString(Util.DEFAULT_UPDATE_SEED));
    }

    /**
     * Constructor for the initial pass of an Algebraic function. Pig will call this and pass the
     * same constructor arguments as the original UDF. In this case the arguments are ignored.
     *
     * @param nomEntriesStr <a href="{@docRoot}/resources/dictionary.html#nomEntries">See Nominal Entries</a>.
     */
    public Initial(final String nomEntriesStr) {
      this(nomEntriesStr, "1.0", Long.toString(Util.DEFAULT_UPDATE_SEED));
    }

    /**
     * Constructor for the initial pass of an Algebraic function. Pig will call this and pass the
     * same constructor arguments as the original UDF. In this case the arguments are ignored.
     *
     * @param nomEntriesStr <a href="{@docRoot}/resources/dictionary.html#nomEntries">See Nominal Entries</a>.
     * @param pStr <a href="{@docRoot}/resources/dictionary.html#p">See Sampling Probability, <i>p</i></a>.
     *
     *
     */
    public Initial(final String nomEntriesStr, final String pStr) {
      this(nomEntriesStr, pStr, Long.toString(Util.DEFAULT_UPDATE_SEED));
    }

    /**
     * Constructor for the initial pass of an Algebraic function. Pig will call this and pass the
     * same constructor arguments as the original UDF. In this case the arguments are ignored.
     *
     * @param nomEntriesStr <a href="{@docRoot}/resources/dictionary.html#nomEntries">See Nominal Entries</a>.
     * @param pStr <a href="{@docRoot}/resources/dictionary.html#p">See Sampling Probability, <i>p</i></a>.
     *
     *
     * @param seedStr <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>.
     */
    public Initial(final String nomEntriesStr, final String pStr, final String seedStr) {}

    @Override  //Initial exec
    public Tuple exec(final Tuple inputTuple) throws IOException { //throws is in API
      return inputTuple;
    }
  }

  // STATIC IntermediateFinal Class only called by Pig

  /*************************************************************************************************
   * Class used to calculate the intermediate or final combiner pass of an <i>Algebraic</i> sketch
   * operation. This is called from the combiner, and may be called multiple times (from the mapper
   * and from the reducer). It will receive a bag of values returned by either the <i>Intermediate</i>
   * stage or the <i>Initial</i> stages, so it needs to be able to differentiate between and
   * interpret both types.
   */
  public static class IntermediateFinal extends EvalFunc<Tuple> {
    //The Algebraic worker classes (Initial, IntermediateFinal) are static and stateless.
    //The constructors and final parameters must mirror the parent class as there is no linkage
    // between them.
    private final int myNomEntries_;
    private final float myP_;
    private final long mySeed_;
    private final Tuple myEmptyCompactOrderedSketchTuple_;

    /**
     * Default constructor to make pig validation happy.  Assumes:
     * <ul>
     * <li><a href="{@docRoot}/resources/dictionary.html#defaultNomEntries">See Default Nominal Entries</a></li>
     * <li><i>p</i> = 1.0. <a href="{@docRoot}/resources/dictionary.html#p">See Sampling Probability,
     * <i>p</i></a>.</li>
     * <li><a href="{@docRoot}/resources/dictionary.html#defaultUpdateSeed">See Default Update Seed</a></li>
     * </ul>
     */
    public IntermediateFinal() {
      this(Integer.toString(Util.DEFAULT_NOMINAL_ENTRIES), "1.0",
          Long.toString(Util.DEFAULT_UPDATE_SEED));
    }

    /**
     * Constructor for the intermediate and final passes of an Algebraic function. Pig will call
     * this and pass the same constructor arguments as the base UDF.  Assumes:
     * <ul>
     * <li><i>p</i> = 1.0. <a href="{@docRoot}/resources/dictionary.html#p">See Sampling Probability,
     * <i>p</i></a>.</li>
     * <li><a href="{@docRoot}/resources/dictionary.html#defaultUpdateSeed">See Default Update Seed</a></li>
     * </ul>
     *
     * @param nomEntriesStr <a href="{@docRoot}/resources/dictionary.html#nomEntries">See Nominal Entries</a>.
     */
    public IntermediateFinal(final String nomEntriesStr) {
      this(nomEntriesStr, "1.0", Long.toString(Util.DEFAULT_UPDATE_SEED));
    }

    /**
     * Constructor for the intermediate and final passes of an Algebraic function. Pig will call
     * this and pass the same constructor arguments as the base UDF.  Assumes:
     * <ul>
     * <li><a href="{@docRoot}/resources/dictionary.html#defaultUpdateSeed">See Default Update Seed</a></li>
     * </ul>
     *
     * @param nomEntriesStr <a href="{@docRoot}/resources/dictionary.html#nomEntries">See Nominal Entries</a>.
     * @param pStr <a href="{@docRoot}/resources/dictionary.html#p">See Sampling Probability, <i>p</i></a>.
     */
    public IntermediateFinal(final String nomEntriesStr, final String pStr) {
      this(nomEntriesStr, pStr, Long.toString(Util.DEFAULT_UPDATE_SEED));
    }

    /**
     * Constructor with strings for the intermediate and final passes of an Algebraic function.
     * Pig will call this and pass the same constructor arguments as the original UDF.
     *
     * @param nomEntriesStr <a href="{@docRoot}/resources/dictionary.html#nomEntries">See Nominal Entries</a>.
     * @param pStr <a href="{@docRoot}/resources/dictionary.html#p">See Sampling Probability, <i>p</i></a>.
     * @param seedStr <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>.
     */
    public IntermediateFinal(final String nomEntriesStr, final String pStr, final String seedStr) {
      this(Integer.parseInt(nomEntriesStr), Float.parseFloat(pStr), Long.parseLong(seedStr));
    }

    /**
     * Constructor with primitives for the intermediate and final passes of an Algebraic function.
     * Pig will call this and pass the same constructor arguments as the Top Level UDF.
     *
     * @param nomEntries <a href="{@docRoot}/resources/dictionary.html#nomEntries">See Nominal Entries</a>.
     * @param p <a href="{@docRoot}/resources/dictionary.html#p">See Sampling Probability, <i>p</i></a>.
     * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>.
     */
    public IntermediateFinal(final int nomEntries, final float p, final long seed) {
      myNomEntries_ = nomEntries;
      myP_ = p;
      mySeed_ = seed;
      myEmptyCompactOrderedSketchTuple_ = emptySketchTuple(seed);
    }

    @Override //IntermediateFinal exec
    public Tuple exec(final Tuple inputTuple) throws IOException { //throws is in API
      final Union union = newUnion(myNomEntries_, myP_, mySeed_);
      final DataBag outerBag = extractBag(inputTuple); //InputTuple.bag0
      if (outerBag == null) { //must have non-empty outer bag at field 0.
        return myEmptyCompactOrderedSketchTuple_; //abort & return empty sketch
      }
      //Bag is not empty.

      for (Tuple dataTuple : outerBag) {
        final Object f0 = extractFieldAtIndex(dataTuple, 0); //inputTuple.bag0.dataTupleN.f0
        //must have non-null field zero
        if (f0 == null) {
          continue; //go to next dataTuple if there is one
        }
        //f0 is not null
        if (f0 instanceof DataBag) {
          final DataBag innerBag = (DataBag)f0; //inputTuple.bag0.dataTupleN.f0:bag
          if (innerBag.size() == 0) { continue; }
          //If field 0 of a dataTuple is a Bag all innerTuples of this inner bag
          // will be passed into the union.
          //It is due to system bagged outputs from multiple mapper Initial functions.
          //The Intermediate stage was bypassed.
          updateUnion(innerBag, union); //process all tuples of innerBag

        }
        else if (f0 instanceof DataByteArray) { //inputTuple.bag0.dataTupleN.f0:DBA
          //If field 0 of a dataTuple is a DataByteArray we assume it is a sketch
          // due to system bagged outputs from multiple mapper Intermediate functions.
          // Each dataTuple.DBA:sketch will merged into the union.
          final DataByteArray dba = ((DataByteArray) f0);
          union.update(Memory.wrap(dba.get()));

        }
        else { // we should never get here.
          throw new IllegalArgumentException("dataTuple.Field0: Is not a DataByteArray: "
              + f0.getClass().getName());
        }
      } //End for
      final CompactSketch compactSketch = union.getResult(true, null);
      return compactOrderedSketchToTuple(compactSketch);
    }

  } //End IntermediateFinal

}
