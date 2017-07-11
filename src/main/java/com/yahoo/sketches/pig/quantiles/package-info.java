/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the Apache License 2.0. See LICENSE file
 * at the project root for terms.
 */

/**
 * Hive UDFs for Quantiles sketches.
 * This includes UDFs for generic ItemsSketch and specialized DoublesSketch.
 * 
 * <p>The generic implementation is in the form of abstract classes DataToItemsSketch and
 * UnionItemsSketch to be specialized for particular types of items.
 * An implementation for strings is provided: DataToStringsSketch, UnionStringsSketch,
 * plus UDFs to obtain the results from sketches:
 * GetQuantileFromStringsSketch, GetQuantilesFromStringsSketch and GetPmfFromStringsSketch.
 * 
 * <p>Support for DoublesSketch: DataToDoublesSketch, UnionDoublesSketch,
 * GetQuantileFromDoublesSketch, GetQuantilesFromDoublesSketch, GetPmfFromDoublesSketch
 *
 * @author Alexander Saydakov
 */
package com.yahoo.sketches.pig.quantiles;
