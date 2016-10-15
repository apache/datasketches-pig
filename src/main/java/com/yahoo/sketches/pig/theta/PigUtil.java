/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.pig.theta;

import java.io.IOException;

import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.yahoo.sketches.ResizeFactor;
import com.yahoo.memory.Memory;
import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.theta.CompactSketch;
import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.UpdateSketch;

/**
 * Common methods for the pig classes.
 * 
 * @author Lee Rhodes
 */
class PigUtil {
  static final ResizeFactor RF = ResizeFactor.X8;
  
  /**
   * Serialize an ordered CompactSketch to SketchTuple 
   * 
   * @param sketch The compact ordered sketch to serialize
   * @return Sketch Tuple.
   */
  static final Tuple compactOrderedSketchToTuple(CompactSketch sketch) {
    Tuple outputTuple = TupleFactory.getInstance().newTuple(1);
    byte[] bytes = sketch.toByteArray();
    DataByteArray dba = new DataByteArray(bytes);
    if (!sketch.isOrdered()) {
      throw new IllegalArgumentException("Given sketch must be ordered.");
    }
    try {
      outputTuple.set(0, dba);
    } 
    catch (IOException e) {
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
  static final Sketch tupleToSketch(Tuple tuple, long seed) {
    DataByteArray sketchDBA = null;
    sketchDBA = (DataByteArray) extractFieldAtIndex(tuple, 0);
    Memory srcMem = new NativeMemory(sketchDBA.get());
    Sketch sketch = Sketch.wrap(srcMem, seed);
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
  static final DataBag extractBag(Tuple tuple) {
    DataBag bag = null;
    try {
      bag = (DataBag) tuple.get(0);
      if (bag.size() == 0) return null;
    } 
    catch (IOException | NullPointerException | IndexOutOfBoundsException e ) {
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
  static final Object extractFieldAtIndex(Tuple tuple, int index) {
    Object fi = null;
    try {
      fi = tuple.get(index);
      fi.hashCode(); //cannot be null
    } 
    catch (IOException | NullPointerException | IndexOutOfBoundsException e) {
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
  static final Byte extractTypeAtIndex(Tuple tuple, int index) {
    Byte type = null;
    try {
      type = tuple.getType(index);
    } 
    catch (IOException | NullPointerException | IndexOutOfBoundsException e) {
      return null;
    }
    return type;
  }
  
  /**
   * Return an empty Compact Ordered Sketch Tuple. Empty sketch is only 8 bytes.
   * @param seed the given seed
   * @return an empty compact ordered sketch tuple
   */
  static final Tuple emptySketchTuple(long seed) {
    UpdateSketch sketch = UpdateSketch.builder().setSeed(seed).setResizeFactor(RF).build(16);
    CompactSketch compOrdSketch = sketch.compact(true, null);
    return compactOrderedSketchToTuple(compOrdSketch);
  }
  
}