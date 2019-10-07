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

package org.apache.datasketches.pig.tuple;

import org.testng.annotations.Test;
import org.testng.Assert;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import org.apache.datasketches.tuple.ArrayOfDoublesUpdatableSketch;
import org.apache.datasketches.tuple.ArrayOfDoublesUpdatableSketchBuilder;

import org.apache.commons.math3.stat.inference.TTest;

import java.io.IOException;
import java.util.Random;

/**
 * Test p-value estimation of two ArrayOfDoublesSketch.
 */
public class ArrayOfDoublesSketchesToPValueEstimatesTest {
    /**
     * Check null input to UDF.
     * @throws IOException from EvalFunc<Tuple>.exec(...)
     */
    @Test
    public void nullInput() throws IOException {
        EvalFunc<Tuple> func = new ArrayOfDoublesSketchesToPValueEstimates();

        Tuple resultTuple = func.exec(null);

        Assert.assertNull(resultTuple);
    }

    /**
     * Check input of empty tuple.
     * @throws IOException from EvalFunc<Tuple>.exec(...)
     */
    @Test
    public void emptyInput() throws IOException {
        EvalFunc<Tuple> func = new ArrayOfDoublesSketchesToPValueEstimates();

        Tuple resultTuple = func.exec(TupleFactory.getInstance().newTuple());

        Assert.assertNull(resultTuple);
    }

    /**
     * Check input of single empty sketch.
     * @throws IOException from EvalFunc<Tuple>.exec(...)
     */
    @Test
    public void oneEmptySketch() throws IOException {
        EvalFunc<Tuple> func = new ArrayOfDoublesSketchesToPValueEstimates();

        ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().build();

        Tuple inputTuple = PigUtil.objectsToTuple(new DataByteArray(sketch.compact().toByteArray()));

        Tuple resultTuple = func.exec(inputTuple);

        Assert.assertNull(resultTuple);
    }

    /**
     * Check input of two empty sketches.
     * @throws IOException from EvalFunc<Tuple>.exec(...)
     */
    @Test
    public void twoEmptySketches() throws IOException {
        EvalFunc<Tuple> func = new ArrayOfDoublesSketchesToPValueEstimates();

        ArrayOfDoublesUpdatableSketch sketchA = new ArrayOfDoublesUpdatableSketchBuilder().build();
        ArrayOfDoublesUpdatableSketch sketchB = new ArrayOfDoublesUpdatableSketchBuilder().build();

        Tuple inputTuple = PigUtil.objectsToTuple(new DataByteArray(sketchA.compact().toByteArray()),
                                                  new DataByteArray(sketchB.compact().toByteArray()));

        Tuple resultTuple = func.exec(inputTuple);

        Assert.assertNull(resultTuple);
    }

    /**
     * Check p-value for the smoker data set. Single metric.
     * @throws IOException from EvalFunc<Tuple>.exec(...)
     */
    @Test
    public void smokerDatasetSingleMetric() throws IOException {
        EvalFunc<Tuple> func = new ArrayOfDoublesSketchesToPValueEstimates();

        // Create the two sketches
        ArrayOfDoublesUpdatableSketch sketchA = new ArrayOfDoublesUpdatableSketchBuilder()
                                                    .setNumberOfValues(1)
                                                    .setNominalEntries(16)
                                                    .build();
        ArrayOfDoublesUpdatableSketch sketchB = new ArrayOfDoublesUpdatableSketchBuilder()
                                                    .setNumberOfValues(1)
                                                    .setNominalEntries(16)
                                                    .build();

        // Sample dataset (smoker/non-smoker brain size)
        double[] groupA = {7.3, 6.5, 5.2, 6.3, 7.0, 5.9, 5.2, 5.0, 4.7, 5.7, 5.7, 3.3, 5.0, 4.6, 4.8, 3.8, 4.6};
        double[] groupB = {4.2, 4.0, 2.6, 4.9, 4.4, 4.4, 5.5, 5.1, 5.1, 3.2, 3.9, 3.2, 4.9, 4.3, 4.8, 2.4, 5.5, 5.5, 3.7};

        // Add values to A sketch
        for (int i = 0; i < groupA.length; i++) {
            sketchA.update(i, new double[] {groupA[i]});
        }

        // Add values to B sketch
        for (int i = 0; i < groupB.length; i++) {
            sketchB.update(i, new double[] {groupB[i]});
        }

        // Convert to a tuple and execute the UDF
        Tuple inputTuple = PigUtil.objectsToTuple(new DataByteArray(sketchA.compact().toByteArray()),
                                                  new DataByteArray(sketchB.compact().toByteArray()));
        Tuple resultTuple = func.exec(inputTuple);

        // Should get 1 p-value back
        Assert.assertNotNull(resultTuple);
        Assert.assertEquals(resultTuple.size(), 1);

        // Check p-value values, with a delta
        Assert.assertEquals((double) resultTuple.get(0), 0.0043, 0.0001);
    }

    /**
     * Check p-value for a large data set.
     * @throws IOException from EvalFunc<Tuple>.exec(...)
     */
    @Test
    public void largeDataSet() throws IOException {
        EvalFunc<Tuple> func = new ArrayOfDoublesSketchesToPValueEstimates();

        // Create the two sketches
        ArrayOfDoublesUpdatableSketch sketchA = new ArrayOfDoublesUpdatableSketchBuilder()
                                                    .setNumberOfValues(1)
                                                    .setNominalEntries(16000)
                                                    .build();
        ArrayOfDoublesUpdatableSketch sketchB = new ArrayOfDoublesUpdatableSketchBuilder()
                                                    .setNumberOfValues(1)
                                                    .setNominalEntries(16000)
                                                    .build();

        // Number of values to use.
        int n = 100000;
        int bShift = 1000;
        double[] a = new double[n];
        double[] b = new double[n];

        // Random number generator
        Random rand = new Random(41L);

        // Add values to A sketch
        for (int i = 0; i < n; i++) {
            double val = rand.nextGaussian();
            sketchA.update(i, new double[] {val});
            a[i] = val;
        }

        // Add values to B sketch
        for (int i = 0; i < n; i++) {
            double val = rand.nextGaussian() + bShift;
            sketchB.update(i, new double[] {val});
            b[i] = val;
        }

        TTest tTest = new TTest();
        double expectedPValue = tTest.tTest(a, b);

        // Convert to a tuple and execute the UDF
        Tuple inputTuple = PigUtil.objectsToTuple(new DataByteArray(sketchA.compact().toByteArray()),
                                                  new DataByteArray(sketchB.compact().toByteArray()));
        Tuple resultTuple = func.exec(inputTuple);

        // Should get 1 p-value back
        Assert.assertNotNull(resultTuple);
        Assert.assertEquals(resultTuple.size(), 1);

        // Check p-value values, with a delta
        Assert.assertEquals((double) resultTuple.get(0), expectedPValue, 0.01);
    }

    /**
     * Check p-value for two metrics at the same time.
     * @throws IOException from EvalFunc<Tuple>.exec(...)
     */
    @Test
    public void twoMetrics() throws IOException {
        EvalFunc<Tuple> func = new ArrayOfDoublesSketchesToPValueEstimates();

        // Create the two sketches
        ArrayOfDoublesUpdatableSketch sketchA = new ArrayOfDoublesUpdatableSketchBuilder()
                                                    .setNumberOfValues(2)
                                                    .setNominalEntries(128)
                                                    .build();
        ArrayOfDoublesUpdatableSketch sketchB = new ArrayOfDoublesUpdatableSketchBuilder()
                                                    .setNumberOfValues(2)
                                                    .setNominalEntries(128)
                                                    .build();

        // Sample dataset (smoker/non-smoker brain size)
        double[] groupA = {7.3, 6.5, 5.2, 6.3, 7.0, 5.9, 5.2, 5.0, 4.7, 5.7, 5.7, 3.3, 5.0, 4.6, 4.8, 3.8, 4.6};
        double[] groupB = {4.2, 4.0, 2.6, 4.9, 4.4, 4.4, 5.5, 5.1, 5.1, 3.2, 3.9, 3.2, 4.9, 4.3, 4.8, 2.4, 5.5, 5.5, 3.7};

        // Add values to A sketch
        for (int i = 0; i < groupA.length; i++) {
            sketchA.update(i, new double[] {groupA[i], i});
        }

        // Add values to B sketch
        for (int i = 0; i < groupB.length; i++) {
            sketchB.update(i, new double[] {groupB[i], i});
        }

        // Convert to a tuple and execute the UDF
        Tuple inputTuple = PigUtil.objectsToTuple(new DataByteArray(sketchA.compact().toByteArray()),
                                                  new DataByteArray(sketchB.compact().toByteArray()));
        Tuple resultTuple = func.exec(inputTuple);

        // Should get 2 p-values back
        Assert.assertNotNull(resultTuple);
        Assert.assertEquals(resultTuple.size(), 2);

        // Check expected p-value values, with a delta
        Assert.assertEquals((double) resultTuple.get(0), 0.0043, 0.0001);
        Assert.assertEquals((double) resultTuple.get(1), 0.58, 0.01);
    }

    /**
     * Check with sketch having only one input.
     * @throws IOException from EvalFunc<Tuple>.exec(...)
     */
    @Test
    public void sketchWithSingleValue() throws IOException {
        EvalFunc<Tuple> func = new ArrayOfDoublesSketchesToPValueEstimates();

        // Create the two sketches
        ArrayOfDoublesUpdatableSketch sketchA = new ArrayOfDoublesUpdatableSketchBuilder()
                                                    .setNumberOfValues(1)
                                                    .setNominalEntries(128)
                                                    .build();
        ArrayOfDoublesUpdatableSketch sketchB = new ArrayOfDoublesUpdatableSketchBuilder()
                                                    .setNumberOfValues(1)
                                                    .setNominalEntries(128)
                                                    .build();

        // Sample dataset
        double[] groupA = {7.3, 6.5, 5.2, 6.3, 7.0, 5.9, 5.2, 5.0, 4.7, 5.7, 5.7, 3.3, 5.0, 4.6, 4.8, 3.8, 4.6};
        double[] groupB = {5.0};

        // Add values to A sketch
        for (int i = 0; i < groupA.length; i++) {
            sketchA.update(i, new double[] {groupA[i]});
        }

        // Add values to B sketch
        for (int i = 0; i < groupB.length; i++) {
            sketchB.update(i, new double[] {groupB[i]});
        }

        // Convert to a tuple and execute the UDF
        Tuple inputTuple = PigUtil.objectsToTuple(new DataByteArray(sketchA.compact().toByteArray()),
                                                  new DataByteArray(sketchB.compact().toByteArray()));
        Tuple resultTuple = func.exec(inputTuple);

        // Should get null back, as one of the sketches had fewer than 2 items
        Assert.assertNull(resultTuple);
    }
}
