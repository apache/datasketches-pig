package com.yahoo.sketches.sampling;

import java.util.ArrayList;

/**
 * @author Jon Malkin
 */
public final class SamplingPigUtil {
  public static <T> ArrayList<T> getRawSamplesAsList(final ReservoirItemsSketch<T> sketch) {
    return sketch.getRawSamplesAsList();
  }

  public static <T> int getHeavyRegionCount(final VarOptItemsSketch<T> sketch) {
    return sketch.getHRegionCount();
  }

  public static <T> double getTotalRWeight(final VarOptItemsSketch<T> sketch) {
    return sketch.getTotalWtR();
  }
}
