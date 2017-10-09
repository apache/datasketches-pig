package com.yahoo.sketches.pig.decomposition;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.decomposition.FrequentDirections;
import org.apache.pig.AccumulatorEvalFunc;
import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.ojalgo.array.Array1D;

/**
 * @author Jon Malkin
 */
public class SegmentFrequentDirectionsUDF
        extends AccumulatorEvalFunc<DataByteArray> implements Algebraic {
  private final int tgtK_;
  private final int tgtD_;
  private FrequentDirections sketch_;

  private final String hdfsPath;
  private final String segmentMapFile;
  private HashMap<Long, Integer> segmentMap;

  /**
   * Frequent Directions constructor.
   *
   * @param kStr String indicating the maximum number rows in the projection matrix
   * @param dStr String indicating number of columns in the projection matrix
   */
  public SegmentFrequentDirectionsUDF(final String kStr, final String dStr,
                                      final String hdfsPath, final String segmentMapFile) {
    tgtK_ = Integer.parseInt(kStr);
    tgtD_ = Integer.parseInt(dStr);
    this.hdfsPath = hdfsPath;
    this.segmentMapFile = segmentMapFile;

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

  @Override
  public List<String> getCacheFiles() {
    final List<String> cacheFiles = new ArrayList<>(1);
    cacheFiles.add(hdfsPath + File.separator + segmentMapFile + "#" + segmentMapFile);
    return cacheFiles;
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

    if (segmentMap == null) {
      segmentMap = loadSegmentMap(segmentMapFile);
    }

    //Array1D<Double> vector = Array1D.PRIMITIVE64.makeZero(tgtD_);
    final double[] vector = new double[tgtD_];

    final DataBag vectorBag = (DataBag) input.get(0);
    for (final Tuple record : vectorBag) {
      final DataBag indices = (DataBag) record.get(0);
      Arrays.fill(vector, 0.0);
      boolean hasValidSegment = false;
      for (Tuple segment : indices) {
        final long segmentId = (long) segment.get(0);
        final Integer idx = segmentMap.get(segmentId);

        if (idx != null) {
          vector[idx] = 1.0;
          hasValidSegment = true;
        }
      }

      // only update if vector is nonzero
      if (hasValidSegment) {
        sketch_.update(vector);
      }
    }
  }

  private static HashMap<Long, Integer> loadSegmentMap(final String segmentMapFile) throws IOException {
    HashMap<Long, Integer> segmentMap = new HashMap<>();
    try (BufferedReader br = new BufferedReader(new FileReader(segmentMapFile))) {
      String line;
      while ((line = br.readLine()) != null) {
        if (line.startsWith("#")) { continue; }
        String[] args = line.split("\t");
        int mappedIdx = Integer.parseInt(args[0]);
        long segmentId = Long.parseLong(args[1]);
        segmentMap.put(segmentId, mappedIdx);
      }
    }
    return segmentMap;
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
    public Initial(final String kStr, final String dStr,
                   final String hdfsPath, final String segmentMapFile) { }

    @Override
    public Tuple exec(final Tuple input) {
      return input;
    }
  }


  /*
   * Only Initial maps segments, so only need to load the file in Initial
   */
  public static class Intermediate extends EvalFunc<Tuple> {
    private final int tgtK_;
    private final int tgtD_;
    private final String hdfsPath;
    private final String segmentMapFile;
    private HashMap<Long, Integer> segmentMap;

    public Intermediate() {
      tgtK_ = 1;
      tgtD_ = 1;
      hdfsPath = null;
      segmentMapFile = null;
    }

    /**
     * Frequent Directions constructor.
     *
     * @param kStr String indicating the maximum number rows in the projection matrix
     * @param dStr String indicating number of columns in the projection matrix
     */
    public Intermediate(final String kStr, final String dStr,
                   final String hdfsPath, final String segmentMapFile) {
      tgtK_ = Integer.parseInt(kStr);
      tgtD_ = Integer.parseInt(dStr);
      this.hdfsPath = hdfsPath;
      this.segmentMapFile = segmentMapFile;

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
    public List<String> getCacheFiles() {
      final List<String> cacheFiles = new ArrayList<>(1);
      cacheFiles.add(hdfsPath + File.separator + segmentMapFile + "#" + segmentMapFile);
      return cacheFiles;
    }

    @Override
    public Tuple exec(final Tuple input) throws IOException {
      if (input == null || input.size() < 1 || input.isNull(0)) {
        return null;
      }

      if (segmentMap == null) {
        segmentMap = loadSegmentMap(segmentMapFile);
      }

      final FrequentDirections fd = FrequentDirections.newInstance(tgtK_, tgtD_);

      //Array1D<Double> vector = Array1D.PRIMITIVE64.makeZero(tgtD_);
      final double[] vector = new double[tgtD_];

      final DataBag mapOutputBag = (DataBag) input.get(0);
      for (final Tuple mapOutput : mapOutputBag) {
        // getting some errors where we seem to have a DataByteArray showing up as
        // input to Intermediate!
        if (mapOutput.get(0) instanceof DataByteArray) {
          DataByteArray sketch = (DataByteArray) mapOutput.get(0);
          Memory mem = Memory.wrap(sketch.get());
          fd.update(FrequentDirections.heapify(mem));
        } else {
          final DataBag recordBag = (DataBag) mapOutput.get(0);
          for (final Tuple record : recordBag) {
            if (record.get(0) instanceof DataByteArray) {
              DataByteArray sketch = (DataByteArray) record.get(0);
              Memory mem = Memory.wrap(sketch.get());
              fd.update(FrequentDirections.heapify(mem));
            } else {
              // assuming vector is all zeros here. True initially, and cleared after each update()
              final DataBag rawSegmentBag = (DataBag) record.get(0);
              boolean hasValidSegment = false;
              for (final Tuple rawSegment : rawSegmentBag) {
                final Long segmentId = (Long) rawSegment.get(0);
                final Integer idx = segmentMap.get(segmentId);

                if (idx != null) {
                  vector[idx] = 1.0;
                  hasValidSegment = true;
                }
              }

              if (hasValidSegment) {
                fd.update(vector);
                Arrays.fill(vector, 0.0);
              }
            }
          }
        }
      }

      final DataByteArray dba = new DataByteArray(fd.toByteArray());
      return TupleFactory.getInstance().newTuple(dba);
    }
  }

  public static class Final extends EvalFunc<DataByteArray> {
    private final int tgtK_;
    private final int tgtD_;

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
    @SuppressWarnings("unused")
    public Final(final String kStr, final String dStr,
                 final String hdfsPath, final String segmentMapFile) {
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
    public DataByteArray exec(final Tuple inputTuple) throws IOException {
      if (inputTuple == null || inputTuple.size() < 1 || inputTuple.isNull(0)) {
        return null;
      }

      final FrequentDirections fd = FrequentDirections.newInstance(tgtK_, tgtD_);

      final DataBag sketches = (DataBag) inputTuple.get(0);
      for (Tuple t : sketches) {
        DataByteArray sketch = (DataByteArray) t.get(0);
        Memory mem = Memory.wrap(sketch.get());

        fd.update(FrequentDirections.heapify(mem));
      }

      return new DataByteArray(fd.toByteArray());
    }
  }


}
