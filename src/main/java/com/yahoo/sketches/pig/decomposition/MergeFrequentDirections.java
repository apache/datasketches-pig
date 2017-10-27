package com.yahoo.sketches.pig.decomposition;

import java.io.IOException;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.decomposition.FrequentDirections;
import org.apache.pig.AccumulatorEvalFunc;
import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

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
    gadget_.reset();
  }

  public void accumulate(final Tuple inputTuple) throws IOException {
    if (inputTuple == null || inputTuple.size() < 1 || inputTuple.isNull(0)) {
      return;
    }
    final DataBag sketches = (DataBag) inputTuple.get(0);
    for (Tuple t : sketches) {
      DataByteArray sketch = (DataByteArray) t.get(0);
      Memory mem = Memory.wrap(sketch.get());

      if (gadget_ == null) {
        gadget_ = FrequentDirections.heapify(mem);
      } else {
        gadget_.update(FrequentDirections.heapify(mem));
      }
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

  // Intermediate merges serialized sketches, identical to Final but with a different
  // return type (per Pig spec since we want a DataByteArray at the end)
  public static class Intermediate extends EvalFunc<Tuple> {
    public Intermediate() {
    }

    @Override
    public Tuple exec(final Tuple inputTuple) throws IOException {

      FrequentDirections fd = null;

      final DataBag sketches = (DataBag) inputTuple.get(0);
      for (Tuple t : sketches) {
        if (t.get(0) instanceof DataBag) {
         final DataBag sketchBag = (DataBag) t.get(0);
         for (Tuple s : sketchBag) {
           DataByteArray sketch = (DataByteArray) s.get(0);
           Memory mem = Memory.wrap(sketch.get());

           if (fd == null) {
             fd = FrequentDirections.heapify(mem);
           } else {
             fd.update(FrequentDirections.heapify(mem));
           }
         }
        } else {
          DataByteArray sketch = (DataByteArray) t.get(0);
          Memory mem = Memory.wrap(sketch.get());

          if (fd == null) {
            fd = FrequentDirections.heapify(mem);
          } else {
            fd.update(FrequentDirections.heapify(mem));
          }
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
        DataByteArray sketch = (DataByteArray) t.get(0);
        Memory mem = Memory.wrap(sketch.get());

        if (fd == null) {
          fd = FrequentDirections.heapify(mem);
        } else {
          fd.update(FrequentDirections.heapify(mem));
        }
      }

      return fd == null ? null : new DataByteArray(fd.toByteArray());
    }
  }
}
