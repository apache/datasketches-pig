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

import java.io.IOException;

import org.apache.datasketches.ResizeFactor;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.CompactSketch;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

/**
 * Common methods for the pig classes.
 */
class PigUtil {
  static final ResizeFactor RF = ResizeFactor.X8;

  /**
   * Serialize an ordered CompactSketch to SketchTuple
   *
   * @param sketch The compact ordered sketch to serialize
   * @return Sketch Tuple.
   */
  static final Tuple compactOrderedSketchToTuple(final CompactSketch sketch) {
    final Tuple outputTuple = TupleFactory.getInstance().newTuple(1);
    final byte[] bytes = sketch.toByteArray();
    final DataByteArray dba = new DataByteArray(bytes);
    if (!sketch.isOrdered()) {
      throw new IllegalArgumentException("Given sketch must be ordered.");
    }
    try {
      outputTuple.set(0, dba);
    }
    catch (final IOException e) {
      throw new IllegalArgumentException("IOException thrown: " + e);
    }
    return outputTuple;
  }

  /**
   * Deserialize a sketch from a tuple and return it.
   *
   * @param tuple The tuple containing the sketch. The tuple should have a single entry that is a
   * DataByteArray.
   * @param seed to check against other sketches.
   * @return A sketch
   */
  static final Sketch tupleToSketch(final Tuple tuple, final long seed) {
    DataByteArray sketchDBA = null;
    sketchDBA = (DataByteArray) extractFieldAtIndex(tuple, 0);
    final Memory srcMem = Memory.wrap(sketchDBA.get());
    final Sketch sketch = Sketch.wrap(srcMem, seed);
    return sketch;
  }

  /**
   * Extract a non-empty DataBag from field 0 of the given tuple.  Caught exceptions include
   * IOException, NullPointerException and IndexOutOfBoundsException. The tuple cannot be null
   * and have at least one field, which cannot be null and contain a DataBag, which must
   * have a size greater than zero.
   * If an exception is caught a null is returned to the caller as a signal.
   * @param tuple the given tuple.
   * @return a DataBag or null.
   */
  static final DataBag extractBag(final Tuple tuple) {
    DataBag bag = null;
    try {
      bag = (DataBag) tuple.get(0);
      if (bag.size() == 0) { return null; }
    }
    catch (final IOException | NullPointerException | IndexOutOfBoundsException e ) {
      return null; //as a signal
    }
    return bag;
  }

  /**
   * Extract a non-null Object from field at index <i>index</i> (0-based) of the given tuple.
   * Caught exceptions include IOException, NullPointerException and IndexOutOfBoundsException.
   * The tuple cannot be null and must have at least <i>index+1</i> fields.
   * The field at <i>index</i> cannot be null.
   * If an exception is caught a null is returned to the caller as a signal.
   * @param tuple the given tuple.
   * @param index the 0-based index of the desired field
   * @return a non-null Object or null.
   */
  static final Object extractFieldAtIndex(final Tuple tuple, final int index) {
    Object fi = null;
    try {
      fi = tuple.get(index);
      fi.hashCode(); //cannot be null
    }
    catch (final IOException | NullPointerException | IndexOutOfBoundsException e) {
      return null; //as a signal
    }
    return fi;
  }

  /**
   * Extract a non-null Byte from getType(index) of the given tuple.  Caught exceptions include
   * IOException, NullPointerException and IndexOutOfBoundsException.
   * If an exception is caught a null is returned to the caller as a signal.
   * @param tuple the given tuple.
   * @param index the 0-based index of the desired field
   * @return a Byte of Pig DataType or null.
   */
  static final Byte extractTypeAtIndex(final Tuple tuple, final int index) {
    Byte type = null;
    try {
      type = tuple.getType(index);
    }
    catch (final IOException | NullPointerException | IndexOutOfBoundsException e) {
      return null;
    }
    return type;
  }

  /**
   * Return an empty Compact Ordered Sketch Tuple. Empty sketch is only 8 bytes.
   * @param seed the given seed
   * @return an empty compact ordered sketch tuple
   */
  static final Tuple emptySketchTuple(final long seed) {
    final UpdateSketch sketch = UpdateSketch.builder().setSeed(seed).setResizeFactor(RF)
            .setNominalEntries(16).build();
    final CompactSketch compOrdSketch = sketch.compact(true, null);
    return compactOrderedSketchToTuple(compOrdSketch);
  }

}
