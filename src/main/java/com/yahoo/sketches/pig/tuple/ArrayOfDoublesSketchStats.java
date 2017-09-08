package com.yahoo.sketches.pig.tuple;

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

import com.yahoo.sketches.tuple.ArrayOfDoublesSketch;
import com.yahoo.sketches.tuple.ArrayOfDoublesSketchIterator;

class ArrayOfDoublesSketchStats {

  /**
   * Convert sketch to summary statistics.
   *
   * @param sketch ArrayOfDoublesSketch to convert to summary statistics.
   * @return An array of SummaryStatistics.
   */
  static SummaryStatistics[] sketchToSummaryStatistics(final ArrayOfDoublesSketch sketch) {
    final SummaryStatistics[] summaryStatistics = new SummaryStatistics[sketch.getNumValues()];
    for (int i = 0; i < sketch.getNumValues(); i++) {
      summaryStatistics[i] = new SummaryStatistics();
    }
    final ArrayOfDoublesSketchIterator it = sketch.iterator();
    while (it.next()) {
      final double[] values = it.getValues();
      for (int i = 0; i < it.getValues().length; i++) {
        summaryStatistics[i].addValue(values[i]);
      }
    }
    return summaryStatistics;
  }

}
