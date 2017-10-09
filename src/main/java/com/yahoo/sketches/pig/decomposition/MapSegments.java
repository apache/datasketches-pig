package com.yahoo.sketches.pig.decomposition;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * @author Jon Malkin
 */
public class MapSegments extends EvalFunc<DataBag> {
  private final String hdfsPath;
  private final String segmentMapFile;
  private HashMap<Long, Long> segmentMap;

  public MapSegments(final String hdfsPath, final String filename) {
    this.hdfsPath = hdfsPath;
    this.segmentMapFile = filename;
  }

  @Override
  public List<String> getCacheFiles() {
    final List<String> cacheFiles = new ArrayList<>(1);
    cacheFiles.add(hdfsPath + File.separator + segmentMapFile + "#" + segmentMapFile);
    return cacheFiles;
  }

  @Override
  public DataBag exec(final Tuple input) throws IOException {
    if (input == null || input.size() < 1 || input.isNull(0)) {
      return null;
    }

    if (segmentMap == null) {
      loadSegmentMap();
    }

    final DataBag inputBag = (DataBag) input.get(0);
    final DataBag outputBag = BagFactory.getInstance().newDefaultBag();

    for (final Tuple segment : inputBag) {
      final long segmentId = (long) segment.get(0);
      final Long idx = segmentMap.get(segmentId);

      if (idx != null) {
        final Tuple mapped = TupleFactory.getInstance().newTuple(idx);
        outputBag.add(mapped);
      }
    }

    return outputBag.size() > 0 ? outputBag : null;
  }

  @Override
  public Schema outputSchema(final Schema input) {
    return input;
  }

  private void loadSegmentMap() throws IOException {
    try (BufferedReader br = new BufferedReader(new FileReader(segmentMapFile))) {
      segmentMap = new HashMap<>();

      String line;
      while ((line = br.readLine()) != null) {
        String[] args = line.split("\t");
        long mappedIdx = Long.parseLong(args[0]);
        long segmentId = Long.parseLong(args[1]);
        segmentMap.put(segmentId, mappedIdx);
      }
    }
  }
}
