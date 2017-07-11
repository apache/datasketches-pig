/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the Apache License 2.0. See LICENSE file
 * at the project root for terms.
 */
/**
 * Pig UDFs for HLL sketches.
 *
 * These UDFs can be used as a replacement of corresponding Theta sketch UDFs.
 * Notice that intersections and A-not-B operations are not supported by the HLL sketch.
 * Also notice a small difference in the output type of DataToSketch and UnionSketch:
 * HLL sketch UDFs return DataByteArray (BYTEARRAY in Pig), but corresponding Theta sketch
 * UDFs return a Tuple with single DataByteArray inside. This was a historical accident,
 * and we are reluctant to break the compatibility with existing scripts. HLL sketch UDFs
 * don't have to keep this compatibility. As a result, HLL sketch UDFs don't need
 * flatten() around them to remove the Tuple, and internally they don't have to spend extra
 * resources to wrap every output DataByteArray into a Tuple.
 *
 * @author Alexander Saydakov
 */
package com.yahoo.sketches.pig.hll;
