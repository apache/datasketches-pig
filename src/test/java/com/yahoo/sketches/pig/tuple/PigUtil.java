/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.pig.tuple;

import java.util.List;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.BagFactory;

public class PigUtil {
  public static Tuple objectsToTuple(Object ... objects) {
    Tuple tuple = TupleFactory.getInstance().newTuple();
    for (Object object: objects) tuple.append(object);
    return tuple;
  }

  public static DataBag tuplesToBag(Tuple ... tuples) {
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    for (Tuple tuple: tuples) bag.add(tuple);
    return bag;
  }

  // wrap a List of Objects into a DataBag of a Tuples (one Object per Tuple)
  public static <T> DataBag listToBagOfTuples(List<T> list) {
      DataBag bag = BagFactory.getInstance().newDefaultBag();
      for (Object object: list) {
          Tuple tuple = TupleFactory.getInstance().newTuple();
          tuple.append(object);
          bag.add(tuple);
      }
      return bag;
  }
}
