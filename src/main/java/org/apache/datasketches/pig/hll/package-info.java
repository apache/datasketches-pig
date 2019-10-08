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
package org.apache.datasketches.pig.hll;
