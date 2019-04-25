/*
 * Copyright 2019, Verizon Media.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

/**
 * Pig UDFs for CPC sketches.
 * This is a distinct-counting sketch that implements the
 * <i>Compressed Probabilistic Counting (CPC, a.k.a FM85)</i> algorithms developed by Kevin Lang in
 * his paper
 * <a href="https://arxiv.org/abs/1708.06839">Back to the Future: an Even More Nearly
 * Optimal Cardinality Estimation Algorithm</a>.
 *
 * @author Alexander Saydakov
 */
package com.yahoo.sketches.pig.cpc;
