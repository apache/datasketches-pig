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

package org.apache.datasketches.pig.sampling;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.datasketches.ArrayOfItemsSerDe;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.pig.data.DataReaderWriter;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.WritableByteArray;

/**
 * This <tt>ArrayOfItemsSerDe</tt> implementation takes advantage of the Pig methods used in
 * Pig's own BinStorage to serialize arbitrary <tt>Tuple</tt> data.
 *
 * @author Jon Malkin
 */
public class ArrayOfTuplesSerDe extends ArrayOfItemsSerDe<Tuple> {
  @Override
  public byte[] serializeToByteArray(final Tuple[] items) {
    final WritableByteArray wba = new WritableByteArray();
    try (final DataOutputStream os = new DataOutputStream(wba)) {
      for (Tuple t : items) {
        // BinInterSedes is more efficient, but only suitable for intermediate data within a job.
        DataReaderWriter.writeDatum(os, t);
      }
    } catch (final IOException e) {
      throw new RuntimeException("Error serializing tuple: " + e.getMessage());
    }

    return wba.getData();
  }

  @Override
  public Tuple[] deserializeFromMemory(final Memory mem, final int numItems) {
    //getArray() is OK for Pig and Hive, where Memory is always backed by a primitive array.
    final byte[] bytes = (byte[]) ((WritableMemory) mem).getArray();
    final int offset = (int) ((WritableMemory) mem).getRegionOffset(0L);
    final int length = (int) mem.getCapacity();

    final Tuple[] result = new Tuple[numItems];
    try (final ByteArrayInputStream bais = new ByteArrayInputStream(bytes, offset, length);
         final DataInputStream dis = new DataInputStream(bais)) {
      for (int i = 0; i < numItems; ++i) {
        // BinInterSedes is more efficient, but only suitable for intermediate data within a job.
        // We know we're getting Tuples back in this case so cast is safe
        result[i] = (Tuple) DataReaderWriter.readDatum(dis);
      }
    } catch (final IOException e) {
      throw new RuntimeException("Error deserializing tuple: " + e.getMessage());
    }

    return result;
  }
}
