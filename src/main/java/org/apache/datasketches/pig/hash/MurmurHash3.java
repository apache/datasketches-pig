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

package org.apache.datasketches.pig.hash;

import static java.lang.Math.min;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.datasketches.hash.MurmurHash3.hash;
import static org.apache.datasketches.hash.MurmurHash3Adaptor.hashToLongs;
import static org.apache.datasketches.hash.MurmurHash3Adaptor.modulo;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * Computes a 128-bit MurmurHash3 hash of data input types: String, DataByteArray, Long, Integer,
 * Double, and Float. The 2nd optional parameter can be a Long or Integer seed. The 3rd optional
 * parameter can be a positive Integer modulus divisor. If the divisor is provided, the Integer
 * modulus remainder is computed on the entire 128-bit hash output treated as if it were a 128-bit
 * positive value.
 */
public class MurmurHash3 extends EvalFunc<Tuple> {
  private static final TupleFactory mTupleFactory = TupleFactory.getInstance();
  private int divisor_ = 0;

  /**
   * Top Level Exec Function.
   * <p>
   * This method accepts an object to be hashed and returns a <i>Hash Result Tuple</i>.
   * </p>
   *
   * <b>Hash Input Tuple</b>
   * <ul>
   * <li>Tuple: TUPLE (Must contain 1 field and may contain 2 or 3): <br>
   * <ul>
   * <li>index 0: Object to be hashed: one of String, DataByteArray, Long, Integer, Double,
   * Float.</li>
   * <li>index 1: Seed: Long or Integer</li>
   * <li>index 2: Modulus divisor: Integer; must be positive.</li>
   * </ul>
   * </li>
   * </ul>
   *
   * <p>
   * Any other input tuple will throw an exception!
   * </p>
   *
   * <b>Hash Result Tuple</b>
   * <ul>
   * <li>Tuple: TUPLE (Contains 3 fields)
   * <ul>
   * <li>index 0: Long: LONG: The 1st 64 bits of the hash.</li>
   * <li>index 1: Long: LONG: The 2nd 64 bits of the hash.</li>
   * <li>index 2: Integer: INTEGER: Result of modulo operation, -1 if not computed.
   * </ul>
   * </li>
   * </ul>
   *
   * @param input Hash Input Tuple. If null or empty exec returns null.
   */
  @Override
  public Tuple exec(final Tuple input) throws IOException {
    if ((input == null) || (input.size() == 0)) {
      return null;
    }
    final Tuple out = mTupleFactory.newTuple(3);
    final long[] hashOut = extractInputs(input);
    if (hashOut == null) {
      return out; //contains 3 nulls
    }
    final long h0 = hashOut[0];
    final long h1 = hashOut[1];
    final int modResult = (divisor_ > 0L) ? modulo(h0, h1, divisor_) : -1;
    out.set(0, Long.valueOf(h0));
    out.set(1, Long.valueOf(h1));
    out.set(2, Integer.valueOf(modResult));
    return out;
  }

  private long[] extractInputs(final Tuple input) throws IOException { //may return null
    final int sw = min(input.size(), 3);
    long seed = 0L;
    Object obj = null;
    long[] hashOut = null;
    switch (sw) {
      case 3: { //modulus divisor
        obj = input.get(2);
        if (obj != null) {
          if (obj instanceof Integer) {
            divisor_ = (Integer) obj;
          }
          else {
            throw new IllegalArgumentException("Modulus divisor must be an Integer.");
          }
          if (divisor_ <= 0) {
            throw new IllegalArgumentException("Modulus divisor must be greater than zero. "
                + divisor_);
          }

        } //divisor may be null. If so it will not be used.
      }
      //$FALL-THROUGH$
      case 2: { //seed
        obj = input.get(1);
        if (obj != null) {
          if (obj instanceof Long) {
            seed = (Long) obj;
          }
          else if (obj instanceof Integer) {
            seed = (Integer) obj;
          }
          else {
            throw new IllegalArgumentException("Seed must be an Integer or Long");
          }
        }
      }
      //$FALL-THROUGH$
      case 1: {
        obj = input.get(0);
        final int type = input.getType(0);
        switch (type) {
          case 1: { //Null type, returns null
            break;
          }
          case 10: { //Integer
            final long[] data = { (Integer) obj };
            hashOut = hash(data, seed);
            break;
          }
          case 15: { //Long
            final long[] data = { (Long) obj };
            hashOut = hash(data, seed);
            break;
          }
          case 20: { //Float
            hashOut = hashToLongs((Float) obj, seed);
            break;
          }
          case 25: { //Double
            hashOut = hashToLongs((Double) obj, seed);
            break;
          }
          case 50: { //BYTEARRAY = DataByteArray
            final DataByteArray dba = (DataByteArray) obj;
            if (dba.size() == 0) {
              break; //Empty return null
            }
            hashOut = hash(dba.get(), seed);
            break;
          }
          case 55: { //CHARARRAY = String
            final String datum = (String) obj;
            if (datum.isEmpty()) {
              break; //Empty return null
            }
            hashOut = hash(datum.getBytes(UTF_8), seed);
            break;
          }
          default: {
            throw new IllegalArgumentException("Cannot use this DataType: " + type);
          }
        }
      }
    }
    return hashOut;
  }

  /**
   * The output consists of two longs, or 128 bits, plus the result of the modulo division if
   * specified.
   */
  @Override
  public Schema outputSchema(final Schema input) {
    if (input != null) {
      try {
        final Schema tupleSchema = new Schema();
        tupleSchema.add(new Schema.FieldSchema("Hash0", DataType.LONG));
        tupleSchema.add(new Schema.FieldSchema("Hash1", DataType.LONG));
        tupleSchema.add(new Schema.FieldSchema("ModuloResult", DataType.INTEGER));
        return new Schema(new Schema.FieldSchema(getSchemaName(this
            .getClass().getName().toLowerCase(), input), tupleSchema, DataType.TUPLE));
      }
      catch (final FrontendException e) {
        //fall through
      }
    }
    return null;
  }

}
