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
package org.apache.datasketches.pig.tuple;
