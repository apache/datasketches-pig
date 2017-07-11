/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the Apache License 2.0. See LICENSE file
 * at the project root for terms.
 */

/**
 * Pig UDFs for Frequent Items sketch.
 * This includes generic implementation in the form of abstract classes DataToFrequentItemsSketch
 * and UnionFrequentItemsSketch to be specialized for particular types of items.
 * An implementation for strings is provided: DataToFrequentStringsSketch and UnionFrequentStringsSketch.
 * FrequentStringsSketchToEstimates is to obtain results from sketches.
 *
 * @author Alexander Saydakov
 */
package com.yahoo.sketches.pig.frequencies;
