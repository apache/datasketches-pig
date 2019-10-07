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

package org.apache.datasketches.pig.sampling;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.datasketches.sampling.ReservoirItemsSketch;
import org.apache.datasketches.sampling.ReservoirItemsUnion;
import org.apache.datasketches.sampling.SamplingPigUtil;
import org.apache.pig.AccumulatorEvalFunc;
import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * This is a Pig UDF that applies reservoir sampling to input tuples. It implements both
 * the <tt>Accumulator</tt> and <tt>Algebraic</tt> interfaces for efficient performance.
 *
 * @author Jon Malkin
 */
@SuppressWarnings("javadoc")
public class ReservoirSampling extends AccumulatorEvalFunc<Tuple> implements Algebraic {
  // defined for test consistency
  static final String N_ALIAS = "n";
  static final String K_ALIAS = "k";
  static final String SAMPLES_ALIAS = "samples";

  private static final int DEFAULT_TARGET_K = 1024;

  private final int targetK_;
  private ReservoirItemsSketch<Tuple> reservoir_;

  /**
   * Reservoir sampling constructor.
   * @param kStr String indicating the maximum number of desired entries in the reservoir.
   */
  public ReservoirSampling(final String kStr) {
    targetK_ = Integer.parseInt(kStr);

    if (targetK_ < 2) {
      throw new IllegalArgumentException("ReservoirSampling requires target reservoir size >= 2: "
              + targetK_);
    }
  }

  ReservoirSampling() { targetK_ = DEFAULT_TARGET_K; }

  @Override
  public Tuple exec(final Tuple inputTuple) throws IOException {
    if ((inputTuple == null) || (inputTuple.size() < 1) || inputTuple.isNull(0)) {
      return null;
    }

    final DataBag samples = (DataBag) inputTuple.get(0);

    // if entire input data fits in reservoir, shortcut result
    if (samples.size() <= targetK_) {
      return createResultTuple(samples.size(), targetK_, samples);
    }
    return super.exec(inputTuple);
  }

  @Override
  public void accumulate(final Tuple inputTuple) throws IOException {
    if ((inputTuple == null) || (inputTuple.size() < 1) || inputTuple.isNull(0)) {
      return;
    }

    final DataBag samples = (DataBag) inputTuple.get(0);

    if (reservoir_ == null) {
      reservoir_ = ReservoirItemsSketch.newInstance(targetK_);
    }

    for (Tuple t : samples) {
      reservoir_.update(t);
    }
  }

  @Override
  public Tuple getValue() {
    if (reservoir_ == null) {
      return null;
    }

    final List<Tuple> data = SamplingPigUtil.getRawSamplesAsList(reservoir_);
    final DataBag sampleBag = BagFactory.getInstance().newDefaultBag(data);

    return createResultTuple(reservoir_.getN(), reservoir_.getK(), sampleBag);
  }

  @Override
  public void cleanup() {
    reservoir_ = null;
  }

  @Override
  public Schema outputSchema(final Schema input) {
    if ((input != null) && (input.size() > 0)) {
      try {
        Schema source = input;

        // if we have a bag, grab one level down to get a tuple
        if ((source.size() == 1) && (source.getField(0).type == DataType.BAG)) {
          source = source.getField(0).schema;
        }

        final Schema recordSchema = new Schema();
        recordSchema.add(new Schema.FieldSchema(N_ALIAS, DataType.LONG));
        recordSchema.add(new Schema.FieldSchema(K_ALIAS, DataType.INTEGER));

        // this should add a bag to the output
        recordSchema.add(new Schema.FieldSchema(SAMPLES_ALIAS, source, DataType.BAG));

        return new Schema(new Schema.FieldSchema(getSchemaName(this
                .getClass().getName().toLowerCase(), source), recordSchema, DataType.TUPLE));
      }
      catch (final FrontendException e) {
        throw new RuntimeException(e);
      }
    }
    return null;
  }

  static Tuple createResultTuple(final long n, final int k, final DataBag samples) {
    final Tuple output = TupleFactory.getInstance().newTuple(3);

    try {
      output.set(0, n);
      output.set(1, k);
      output.set(2, samples);
    } catch (final ExecException e) {
      throw new RuntimeException("Pig error: " + e.getMessage(), e);
    }

    return output;
  }


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

  public static class Initial extends EvalFunc<Tuple> {
    private final int targetK_;

    public Initial() {
      targetK_ = DEFAULT_TARGET_K;
    }

    /**
     * Map-side constructor for reservoir sampling UDF
     * @param kStr String indicating the maximum number of desired entries in the reservoir.
     * */
    public Initial(final String kStr) {
      targetK_ = Integer.parseInt(kStr);

      if (targetK_ < 2) {
        throw new IllegalArgumentException("ReservoirSampling requires target reservoir size >= 2: "
                + targetK_);
      }
    }

    @Override
    public Tuple exec(final Tuple inputTuple) throws IOException {
      if ((inputTuple == null) || (inputTuple.size() < 1) || inputTuple.isNull(0)) {
        return null;
      }

      final DataBag records = (DataBag) inputTuple.get(0);

      final ReservoirItemsSketch<Tuple> reservoir;
      final DataBag outputBag;
      int k = targetK_;
      if (records.size() <= targetK_) {
        outputBag = records;
      } else {
        reservoir = ReservoirItemsSketch.newInstance(targetK_);
        for (Tuple t : records) {
          reservoir.update(t);
        }
        // newDefaultBag(List<Tuple>) does *not* copy values
        final List<Tuple> data = SamplingPigUtil.getRawSamplesAsList(reservoir);
        outputBag = BagFactory.getInstance().newDefaultBag(data);
        k = reservoir.getK();
      }

      final Tuple output = TupleFactory.getInstance().newTuple(3);
      output.set(0, records.size());
      output.set(1, k);
      output.set(2, outputBag);

      return output;
    }
  }

  public static class IntermediateFinal extends EvalFunc<Tuple> {
    private final int targetK_;

    public IntermediateFinal() {
      targetK_ = DEFAULT_TARGET_K;
    }

    /**
     * Combiner and reducer side constructor for reservoir sampling UDF
     * @param kStr String indicating the maximum number of desired entries in the reservoir.
     * */
    public IntermediateFinal(final String kStr) {
      targetK_ = Integer.parseInt(kStr);

      if (targetK_ < 2) {
        throw new IllegalArgumentException("ReservoirSampling requires target reservoir size >= 2: "
                + targetK_);
      }
    }

    @Override
    public Tuple exec(final Tuple inputTuple) throws IOException {
      if ((inputTuple == null) || (inputTuple.size() < 1) || inputTuple.isNull(0)) {
        return null;
      }

      final ReservoirItemsUnion<Tuple> union = ReservoirItemsUnion.newInstance(targetK_);

      final DataBag outerBag = (DataBag) inputTuple.get(0);
      for (Tuple reservoir : outerBag) {
        final long n = (long) reservoir.get(0);
        final int k  = (int) reservoir.get(1);

        if ((n <= k) && (k <= targetK_)) {
          for (Tuple t : (DataBag) reservoir.get(2)) {
            union.update(t);
          }
        } else {
          final ArrayList<Tuple> samples = dataBagToArrayList((DataBag) reservoir.get(2));
          union.update(n, k, samples);
        }
      }

      final ReservoirItemsSketch<Tuple> result = union.getResult();
      final ArrayList<Tuple> data = SamplingPigUtil.getRawSamplesAsList(result);
      final DataBag sampleBag = BagFactory.getInstance().newDefaultBag(data);

      final Tuple output = TupleFactory.getInstance().newTuple(3);
      output.set(0, result.getN());
      output.set(1, result.getK());
      output.set(2, sampleBag);

      return output;
    }
  }

  static ArrayList<Tuple> dataBagToArrayList(final DataBag bag) {
    final int arrayLength = (int) bag.size();
    final ArrayList<Tuple> output = new ArrayList<>(arrayLength);

    for (Tuple t : bag) {
      output.add(t);
    }

    return output;
  }
}
