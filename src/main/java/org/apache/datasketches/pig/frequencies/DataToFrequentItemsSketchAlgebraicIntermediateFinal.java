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

package org.apache.datasketches.pig.frequencies;

import java.io.IOException;

import org.apache.datasketches.ArrayOfItemsSerDe;
import org.apache.datasketches.frequencies.ItemsSketch;
import org.apache.log4j.Logger;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

/**
 * Class used to calculate the intermediate pass (combiner) or the final pass
 * (reducer) of an Algebraic sketch operation. This may be called multiple times
 * (from the mapper and from the reducer). It will receive a bag of values
 * returned by either the Intermediate stage or the Initial stages, so
 * it needs to be able to differentiate between and interpret both types.
 * @param <T> Type of the item
 */
public abstract class DataToFrequentItemsSketchAlgebraicIntermediateFinal<T> extends EvalFunc<Tuple> {

  private int sketchSize_;
  private ArrayOfItemsSerDe<T> serDe_;
  private boolean isFirstCall_ = true;

  /**
   * Default constructor to make pig validation happy.
   */
  public DataToFrequentItemsSketchAlgebraicIntermediateFinal() {}

  @SuppressWarnings("javadoc")
  public DataToFrequentItemsSketchAlgebraicIntermediateFinal(
      final int sketchSize, final ArrayOfItemsSerDe<T> serDe) {
    sketchSize_ = sketchSize;
    serDe_ = serDe;
  }

  @Override
  public Tuple exec(final Tuple inputTuple) throws IOException {
    if (isFirstCall_) {
      // this is to see in the log which way was used by Pig
      Logger.getLogger(getClass()).info("algebraic was used");
      isFirstCall_ = false;
    }
    final ItemsSketch<T> sketch = new ItemsSketch<>(sketchSize_);

    final DataBag bag = (DataBag) inputTuple.get(0);
    for (Tuple dataTuple: bag) {
      final Object item = dataTuple.get(0);
      if (item instanceof DataBag) {
        // this is a bag from the Initial function.
        // just insert each item of the tuple into the sketch
        DataToFrequentItemsSketch.updateSketch((DataBag)item, sketch);
      } else if (item instanceof DataByteArray) {
        // This is a sketch from a prior call to the
        // Intermediate function. merge it with the
        // current sketch.
        final ItemsSketch<T> incomingSketch = Util.deserializeSketchFromTuple(dataTuple, serDe_);
        sketch.merge(incomingSketch);
      } else {
        // we should never get here.
        throw new IllegalArgumentException(
            "InputTuple.Field0: Bag contains unrecognized types: " + item.getClass().getName());
      }
    }
    return Util.serializeSketchToTuple(sketch, serDe_);
  }

}
