/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.pig.tuple;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.tuple.ArrayOfDoublesSketch;
import com.yahoo.sketches.tuple.ArrayOfDoublesSketches;

import java.io.IOException;

import org.apache.commons.math3.stat.inference.TTest;
import org.apache.commons.math3.stat.StatUtils;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

/**
 * Calculate p-values given two ArrayOfDoublesSketch. Each value in the sketch
 * is treated as a separate metric measurement, and a p-value will be generated
 * for each metric.
 *
 * The t-statistic is calculated as follows: t = (m1 - m2) / sqrt(var1/n1 + var2/n2)
 *
 * Degrees of freedom is approximated by Welch-Satterthwaite.
 */
public class ArrayOfDoublesSketchesToPValueEstimates extends EvalFunc<Tuple> {
    @Override
    public Tuple exec(final Tuple input) throws IOException {
        if ((input == null) || (input.size() != 2)) {
            return null;
        }

        // Get the two sketches
        final DataByteArray dbaA = (DataByteArray) input.get(0);
        final DataByteArray dbaB = (DataByteArray) input.get(1);
        final ArrayOfDoublesSketch sketchA = ArrayOfDoublesSketches.wrapSketch(Memory.wrap(dbaA.get()));
        final ArrayOfDoublesSketch sketchB = ArrayOfDoublesSketches.wrapSketch(Memory.wrap(dbaB.get()));

        // Check that the size of the arrays in the sketches are the same
        if (sketchA.getNumValues() != sketchB.getNumValues()) {
            throw new IllegalArgumentException("Both sketches must have the same number of values");
        }

        // Check if either sketch is empty
        if (sketchA.isEmpty() || sketchB.isEmpty()) {
            return null;
        }

        // Check that each sketch has at least 2 values
        if (sketchA.getRetainedEntries() < 2 || sketchB.getRetainedEntries() < 2) {
            return null;
        }

        // Get the values from each sketch
        double[][] valuesA = sketchA.getValues();
        double[][] valuesB = sketchB.getValues();
        valuesA = rotateMatrix(valuesA);
        valuesB = rotateMatrix(valuesB);

        // Calculate the p-values
        double[] pValues = new double[valuesA.length];
        TTestWithSketch tTest = new TTestWithSketch();
        for (int i = 0; i < valuesA.length; i++) {
            // Do some special math to get an accurate mean from the sketches
            // Take the sum of the values, divide by theta, then divide by the
            // estimate number of records.
            double meanA = (StatUtils.sum(valuesA[i]) / sketchA.getTheta()) / sketchA.getEstimate();
            double meanB = (StatUtils.sum(valuesB[i]) / sketchB.getTheta()) / sketchB.getEstimate();
            // Variance is based only on the samples we have in the tuple sketch
            double varianceA = StatUtils.variance(valuesA[i]);
            double varianceB = StatUtils.variance(valuesB[i]);
            // Use the estimated number of uniques
            double numA = sketchA.getEstimate();
            double numB = sketchB.getEstimate();
            pValues[i] = tTest.callTTest(meanA, meanB, varianceA, varianceB, numA, numB);
        }

        return Util.doubleArrayToTuple(pValues);
    }

    /**
     * Perform a clockwise rotation the output values from the tuple sketch.
     *
     * @param m Input matrix to rotate
     * @return Rotated matrix
     */
    private static double[][] rotateMatrix(double[][] m) {
        final int width = m.length;
        final int height = m[0].length;
        double[][] result = new double[height][width];
        for (int i = 0; i < width; i++) {
            for (int j = 0; j < height; j++) {
                result[j][width - 1 - i] = m[i][j];
            }
        }
        return result;
    }

    /**
     * This class exists to get around the protected tTest method.
     */
    private class TTestWithSketch extends TTest {
        /**
         * Call the protected tTest method.
         */
        public double callTTest(double m1, double m2, double v1, double v2, double n1, double n2) {
            return super.tTest(m1, m2, v1, v2, n1, n2);
        }
    }
}
