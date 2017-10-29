package com.yahoo.sketches.pig.decomposition;

import java.io.IOException;
import java.util.Arrays;

import org.apache.pig.AccumulatorEvalFunc;
import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.decomposition.FrequentDirections;

/**
 * @author Jon Malkin
 */
public class DataToSketch extends AccumulatorEvalFunc<DataByteArray> implements Algebraic {
  private final int tgtK_;
  private final int tgtD_;
  private FrequentDirections sketch_;

  /**
   * Frequent Directions constructor.
   *
   * @param kStr String indicating the maximum number rows in the projection matrix
   * @param dStr String indicating number of columns in the projection matrix
   */
  public DataToSketch(final String kStr, final String dStr) {
    tgtK_ = Integer.parseInt(kStr);
    tgtD_ = Integer.parseInt(dStr);

    if (tgtK_ < 1) {
      throw new IllegalArgumentException("FrequentDirections requires target output size >= 1: "
              + tgtK_);
    }
    if (tgtD_ < 1) {
      throw new IllegalArgumentException("FrequentDirections requires input dimensionality >= 1: "
              + tgtD_);
    }

    sketch_ = FrequentDirections.newInstance(tgtK_, tgtD_);
  }

  // should never be used
  DataToSketch() {
    tgtK_ = 1;
    tgtD_ = 2;
    sketch_ = FrequentDirections.newInstance(tgtK_, tgtD_); // to avoid null checks in getValue() and cleanup()
  }

  public DataByteArray getValue() {
    if (sketch_.isEmpty()) {
      return null;
    } else {
      return new DataByteArray(sketch_.toByteArray());
    }
  }

  public void cleanup() {
    sketch_.reset();
  }

  public void accumulate(final Tuple input) throws IOException {
    if (input == null || input.size() < 1 || input.isNull(0)) {
      return;
    }

    final DataBag outerBag = (DataBag) input.get(0);
    if (outerBag.size() == 0) { return; }

    final double[] vector = new double[tgtD_];

    for (final Tuple record : outerBag) {
      final DataBag elements = (DataBag) record.get(0);
      Arrays.fill(vector, 0.0);

      for (Tuple entry : elements) {
        final int dimId = (int) entry.get(0);
        final double value = (double) entry.get(1);
        vector[dimId] = value;
      }

      sketch_.update(vector);
    }
  }

  @Override
  public String getInitial() {
    return Initial.class.getName();
  }

  @Override
  public String getIntermed() {
    return Intermediate.class.getName();
  }

  @Override
  public String getFinal() {
    return Final.class.getName();
  }

  public static class Initial extends EvalFunc<Tuple> {

    public Initial() {
    }

    @SuppressWarnings("unused")
    public Initial(final String kStr, final String dStr) {
    }

    @Override
    public Tuple exec(final Tuple input) {
      return input;
    }
  }


  public static class Intermediate extends EvalFunc<Tuple> {
    private final int tgtK_;
    private final int tgtD_;


    // unused, but required to set values in default constructor
    public Intermediate() {
      tgtK_ = 1;
      tgtD_ = 1;
    }

    /**
     * Frequent Directions constructor.
     *
     * @param kStr String indicating the maximum number rows in the projection matrix
     * @param dStr String indicating number of columns in the projection matrix
     */
    public Intermediate(final String kStr, final String dStr) {
      tgtK_ = Integer.parseInt(kStr);
      tgtD_ = Integer.parseInt(dStr);

      if (tgtK_ < 1) {
        throw new IllegalArgumentException("FrequentDirections requires target output size >= 1: "
                + tgtK_);
      }
      if (tgtD_ < 1) {
        throw new IllegalArgumentException("FrequentDirections requires input dimensionality >= 1: "
                + tgtD_);
      }
    }

    @Override
    public Tuple exec(final Tuple input) throws IOException {
      if (input == null || input.size() < 1 || input.isNull(0)) {
        return null;
      }

      final DataBag outerBag = (DataBag) input.get(0);
      if (outerBag.size() == 0) {
        return null;
      }

      final FrequentDirections fd = FrequentDirections.newInstance(tgtK_, tgtD_);
      double[] vector = null;

      for (final Tuple recordTuple : outerBag) {
        final Object field0 = recordTuple.get(0);
        if (field0 instanceof DataByteArray) {
          // input is serialized sketch
          DataByteArray sketchBytes = (DataByteArray) field0; // input.outerBag.recordTuple.field0:DataByteArray
          Memory mem = Memory.wrap(sketchBytes.get());
          fd.update(FrequentDirections.heapify(mem));
        } else if (field0 instanceof DataBag) {
          final DataBag dataBag = (DataBag) field0; // input.outerBag.recordTuple.field0:DataBag

          if (vector == null) {
            vector = new double[tgtD_];
          } else {
            Arrays.fill(vector, 0.0);
          }

          for (final Tuple dataTuple : dataBag) {
            // input is (dim, value) pair
            final int dimId = (int) dataTuple.get(0);
            final double value = (double) dataTuple.get(1);
            vector[dimId] = value;
          }
          fd.update(vector);
        } else {
          throw new IllegalArgumentException("dataTuple.Field0: Is neither a DataByteArray nor a DataBag: "
                  + field0.getClass().getName());
        }
      }

      final DataByteArray dba = new DataByteArray(fd.toByteArray());
      return TupleFactory.getInstance().newTuple(dba);
    }
  }

  public static class Final extends EvalFunc<DataByteArray> {
    private final int tgtK_;
    private final int tgtD_;

    // unused, but required to set values in default constructor
    public Final() {
      tgtK_ = 1;
      tgtD_ = 1;
    }

    /**
     * Frequent Directions constructor.
     *
     * @param kStr String indicating the maximum number rows in the projection matrix
     * @param dStr String indicating number of columns in the projection matrix
     */
    public Final(final String kStr, final String dStr) {
      tgtK_ = Integer.parseInt(kStr);
      tgtD_ = Integer.parseInt(dStr);

      if (tgtK_ < 1) {
        throw new IllegalArgumentException("FrequentDirections requires target output size >= 1: "
                + tgtK_);
      }
      if (tgtD_ < 1) {
        throw new IllegalArgumentException("FrequentDirections requires input dimensionality >= 1: "
                + tgtD_);
      }
    }

    @Override
    public DataByteArray exec(final Tuple input) throws IOException {
      if (input == null || input.size() < 1 || input.isNull(0)) {
        return null;
      }

      final FrequentDirections fd = FrequentDirections.newInstance(tgtK_, tgtD_);
      final DataBag sketchesBag = (DataBag) input.get(0);

      for (Tuple sketchTuple : sketchesBag) {
        DataByteArray sketch = (DataByteArray) sketchTuple.get(0);
        Memory mem = Memory.wrap(sketch.get());
        fd.update(FrequentDirections.heapify(mem));
      }

      return new DataByteArray(fd.toByteArray());
    }
  }
}
