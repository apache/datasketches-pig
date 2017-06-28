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
    final DataOutputStream os = new DataOutputStream(wba);
    try {
      for (Tuple t : items) {
        // BinInterSedes is more efficient, but only suitable for intermediate data within a job
        DataReaderWriter.writeDatum(os, t);
      }
    } catch (final IOException e) {
      throw new RuntimeException("Error serializing tuple: " + e.getMessage());
    }

    return wba.getData();
  }

  @Override
  public Tuple[] deserializeFromMemory(final Memory mem, final int numItems) {
    // if we could get the correct offset into the region, the following avoids a copy:
    //final byte[] bytes = (byte[]) ((WritableMemory) mem).getArray();
    final int size = (int) mem.getCapacity();
    final byte[] bytes = new byte[size];
    mem.getByteArray(0, bytes, 0, size);

    final DataInputStream is = new DataInputStream(new ByteArrayInputStream(bytes));

    final Tuple[] result = new Tuple[numItems];
    try {
      for (int i = 0; i < numItems; ++i) {
        // BinInterSedes is more efficient, but only suitable for intermediate data within a job
        // we know we're getting Tuples back in this case
        result[i] = (Tuple) DataReaderWriter.readDatum(is);
      }
    } catch (final IOException e) {
      throw new RuntimeException("Error deserializing tuple: " + e.getMessage());
    }

    return result;
  }
}
