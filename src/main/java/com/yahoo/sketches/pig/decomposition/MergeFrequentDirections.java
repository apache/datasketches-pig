package com.yahoo.sketches.pig.decomposition;

import java.io.IOException;

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
public class MergeFrequentDirections
        extends AccumulatorEvalFunc<DataByteArray> implements Algebraic {
  private FrequentDirections gadget_;

  public MergeFrequentDirections() {
  }

  public DataByteArray getValue() {
    if (gadget_ == null || gadget_.isEmpty()) {
      return null;
    } else {
      return new DataByteArray(gadget_.toByteArray());
    }
  }

  public void cleanup() {
    if (gadget_ != null) {
      gadget_.reset();
    }
  }

  public void accumulate(final Tuple inputTuple) throws IOException {
    if (inputTuple == null || inputTuple.size() < 1 || inputTuple.isNull(0)) {
      return;
    }
    final DataBag sketches = (DataBag) inputTuple.get(0);
    for (Tuple t : sketches) {
      gadget_ = wrapAndMergeSketch(gadget_, (DataByteArray) t.get(0));
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

  // Initial is a simple pass-through to the combiner/reducer
  public static class Initial extends EvalFunc<Tuple> {

    public Initial() {
    }

    @Override
    public Tuple exec(final Tuple input) {
      return input;
    }
  }

  public static class Intermediate extends EvalFunc<Tuple> {
    public Intermediate() {
    }

    @Override
    public Tuple exec(final Tuple inputTuple) throws IOException {
      FrequentDirections fd = null;

      final DataBag sketches = (DataBag) inputTuple.get(0);

      // can get a bag of serialized sketches directly from Initial or else from earlier Intermediate calls
      for (Tuple t : sketches) {
        Object field0 = t.get(0);
        if (field0 instanceof DataBag) {
         final DataBag sketchBag = (DataBag) t.get(0);
         for (Tuple s : sketchBag) {
           fd = wrapAndMergeSketch(fd, (DataByteArray) s.get(0));
         }
        } else if (field0 instanceof DataByteArray) {
          fd = wrapAndMergeSketch(fd, (DataByteArray) t.get(0));
        } else {
          throw new IllegalArgumentException("dataTuple.Field0: Is neither a DataByteArray nor a DataBag: "
                  + field0.getClass().getName());
        }
      }

      if (fd == null) {
        return null;
      }

      final DataByteArray dba = new DataByteArray(fd.toByteArray());
      return TupleFactory.getInstance().newTuple(dba);
    }
  }

  public static class Final extends EvalFunc<DataByteArray> {
    public Final() {
    }

    @Override
    public DataByteArray exec(final Tuple inputTuple) throws IOException {
      FrequentDirections fd = null;

      final DataBag sketches = (DataBag) inputTuple.get(0);
      for (Tuple t : sketches) {
        fd = wrapAndMergeSketch(fd, (DataByteArray) t.get(0));
      }

      return fd == null ? null : new DataByteArray(fd.toByteArray());
    }
  }

  // assumes Tuple has a DataByteArray in position 0
  private static FrequentDirections wrapAndMergeSketch(FrequentDirections gadget, final DataByteArray sketch) {
    final Memory mem = Memory.wrap(sketch.get());

    if (gadget == null) {
      gadget = FrequentDirections.heapify(mem);
    } else {
      gadget.update(FrequentDirections.heapify(mem));
    }

    return gadget;
  }
}
