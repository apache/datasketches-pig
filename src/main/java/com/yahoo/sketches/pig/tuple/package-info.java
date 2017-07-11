/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the Apache License 2.0. See LICENSE file
 * at the project root for terms.
 */
/**
 * Pig UDFs for Tuple sketches.
 * Tuple sketches are based on the idea of Theta sketches with the addition of
 * values associated with unique keys.
 * Two sets of tuple sketch classes are available at the moment:
 * generic Tuple sketches with user-defined Summary, and a faster specialized
 * implementation with an array of double values.
 *
 * <p>There are two sets of Pig UDFs: one for generic Tuple sketch with an example
 * implementation for DoubleSummay, and another one for a specialized ArrayOfDoublesSketch.
 * 
 * <p> The generic implementation is in the form of abstract classes DataToSketch and
 * UnionSketch to be specialized for particular types of Summary.
 * An example implementation for DoubleSumamry is provided: DataToDoubleSummarySketch and
 * UnionDoubleSummarySketch, as well as UDFs to obtain the results from sketches:
 * DoubleSumamrySketchToEstimates and DoubleSummarySketchToPercentile.
 * 
 * <p>UDFs for ArrayOfDoublesSketch: DataToArrayOfDoublesSketch, UnionArrayOfDoublesSketch,
 * ArrayOfDoublesSketchToEstimates.
 * 
 * @author Alexander Saydakov
 */
package com.yahoo.sketches.pig.tuple;
