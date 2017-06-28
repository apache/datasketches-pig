/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.pig.tuple;

import java.util.List;

import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class PigUtil {
  /**
   * Wraps input Objects in a Tuple
   * @param objects Objects intended to be wrapped
   * @return a Pig Tuple containing the input Objects
   */
  public static Tuple objectsToTuple(final Object ... objects) {
    final Tuple tuple = TupleFactory.getInstance().newTuple();
    for (Object object: objects) { tuple.append(object); }
    return tuple;
  }

  /**
   * Wraps a set of Tuples in a DataBag
   * @param tuples Tuples to wrap
   * @return a Pig DataBag containing the input Tuples
   */
  public static DataBag tuplesToBag(final Tuple ... tuples) {
    final DataBag bag = BagFactory.getInstance().newDefaultBag();
    for (Tuple tuple: tuples) { bag.add(tuple); }
    return bag;
  }

  /**
   * Wraps a List of objects into a DataBag of Tuples (one object per Tuple)
   * @param list List of items to wrap
   * @param <T> Type of objects in the List
   * @return a Pig DataBag containing Tuples populated with list items
   */
  public static <T> DataBag listToBagOfTuples(final List<T> list) {
      final DataBag bag = BagFactory.getInstance().newDefaultBag();
      for (Object object: list) {
          final Tuple tuple = TupleFactory.getInstance().newTuple();
          tuple.append(object);
          bag.add(tuple);
      }
      return bag;
  }
}
