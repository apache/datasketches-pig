package com.yahoo.sketches.sampling;

import java.util.ArrayList;

/**
 * @author Jon Malkin
 */
public final class SamplingPigUtil {
  public static <T> ArrayList<T> getRawSamplesAsList(final ReservoirItemsSketch<T> sketch) {
    return sketch.getRawSamplesAsList();
  }
}
