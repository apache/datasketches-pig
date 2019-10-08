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

package org.apache.datasketches.pig.tuple;

import java.util.List;

import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

@SuppressWarnings("javadoc")
public class PigUtil {
  /**
   * Wraps input Objects in a Tuple
   * @param objects Objects intended to be wrapped
   * @return a Pig Tuple containing the input Objects
   */
  public static Tuple objectsToTuple(final Object ... objects) {
    final Tuple tuple = TupleFactory.getInstance().newTuple();
    for (final Object object: objects) { tuple.append(object); }
    return tuple;
  }

  /**
   * Wraps a set of Tuples in a DataBag
   * @param tuples Tuples to wrap
   * @return a Pig DataBag containing the input Tuples
   */
  public static DataBag tuplesToBag(final Tuple ... tuples) {
    final DataBag bag = BagFactory.getInstance().newDefaultBag();
    for (final Tuple tuple: tuples) { bag.add(tuple); }
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
      for (final Object object: list) {
          final Tuple tuple = TupleFactory.getInstance().newTuple();
          tuple.append(object);
          bag.add(tuple);
      }
      return bag;
  }
}
