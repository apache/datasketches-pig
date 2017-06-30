/*
 * Copyright 2017, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.pig.sampling;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.pig.data.DataReaderWriter;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.WritableByteArray;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.ArrayOfItemsSerDe;

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
