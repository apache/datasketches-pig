/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.pig.theta;

import static com.yahoo.sketches.Family.A_NOT_B;
import static com.yahoo.sketches.Family.QUICKSELECT;

import java.io.IOException;

import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.NativeMemory;
import com.yahoo.sketches.theta.AnotB;
import com.yahoo.sketches.theta.CompactSketch;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.theta.Intersection;
import com.yahoo.sketches.theta.SetOperation;
import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.Union;
import com.yahoo.sketches.theta.UpdateSketch;
import com.yahoo.sketches.theta.ResizeFactor;

/**
 * Common methods for the pig classes.
 * 
 * @author Lee Rhodes
 */
class PigUtil {
  private static final ResizeFactor RF = ResizeFactor.X8;
  
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
      throw new IllegalArgumentException("IOException thrown: "+e);
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
    Sketch sketch = Sketch.heapify(srcMem, seed);
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
   * Return an empty Compact Ordered Sketch Tuple.
   * @param seed the given seed
   * @return an empty compact ordered sketch tuple
   */
  static final Tuple emptySketchTuple(long seed) {
    UpdateSketch sketch = newUpdateSketch(16,(float)1.0, seed);
    CompactSketch compOrdSketch = sketch.compact(true, null);
    return compactOrderedSketchToTuple(compOrdSketch);
  }
  
  /**
   * Return a new empty HeapQuickSelectSketch
   * @param nomEntries the given nominal entries
   * @param p the given probability p
   * @param seed the given seed
   * @return a new empty HeapQuickSelectSketch
   */
  static final UpdateSketch newUpdateSketch(int nomEntries, float p, long seed) {
    return UpdateSketch.builder().setSeed(seed).setP(p).setResizeFactor(RF).
        setFamily(QUICKSELECT).build(nomEntries);
  }
  
  /**
   * Return a new Union operation
   * @param nomEntries the given entries
   * @param p the given probability p
   * @param seed the given seed
   * @return a new Union
   */
  static final Union newUnion(int nomEntries, float p, long seed) {
    return (Union) SetOperation.builder().setP(p).setSeed(seed).
        setResizeFactor(RF).build(nomEntries, Family.UNION);
  }
  
  /**
   * Return a new Intersection operation
   * @param nomEntries the given entries
   * @param p the given probability p
   * @param seed the given seed
   * @return a new Intersection
   */
  static final Intersection newIntersection(int nomEntries, float p, long seed) {
    return (Intersection) SetOperation.builder().setP(p).setSeed(seed).
        setResizeFactor(RF).build(nomEntries, Family.INTERSECTION);
  }
  
  /**
   * Return a new AnotB operation
   * @param seed the given seed
   * @return a new Intersection
   */
  static final AnotB newAnotB(long seed) {
    return (AnotB) SetOperation.builder().setSeed(seed).build(A_NOT_B);
  }
  
}