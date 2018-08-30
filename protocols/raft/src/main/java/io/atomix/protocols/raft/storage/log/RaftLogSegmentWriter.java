/*
 * Copyright 2017-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.protocols.raft.storage.log;

import io.atomix.protocols.raft.storage.log.index.RaftLogIndex;
import io.atomix.storage.StorageException;
import io.atomix.storage.buffer.Bytes;
import io.atomix.utils.serializer.Namespace;

import java.io.File;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

/**
 * Segment writer.
 * <p>
 * The format of an entry in the log is as follows:
 * <ul>
 * <li>64-bit index</li>
 * <li>8-bit boolean indicating whether a term change is contained in the entry</li>
 * <li>64-bit optional term</li>
 * <li>32-bit signed entry length, including the entry type ID</li>
 * <li>8-bit signed entry type ID</li>
 * <li>n-bit entry bytes</li>
 * </ul>
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class RaftLogSegmentWriter<E> implements RaftLogWriter<E> {
  private final File file;
  private final FileChannel channel;
  private final RaftLogSegmentDescriptor descriptor;
  private final int maxEntrySize;
  private final RaftLogIndex index;
  private final Namespace namespace;
  private final ByteBuffer memory;
  private final long firstIndex;
  private Indexed<E> lastEntry;

  public RaftLogSegmentWriter(
      File file,
      FileChannel channel,
      RaftLogSegmentDescriptor descriptor,
      int maxEntrySize,
      RaftLogIndex index,
      Namespace namespace) {
    this.file = file;
    this.channel = channel;
    this.descriptor = descriptor;
    this.maxEntrySize = maxEntrySize;
    this.index = index;
    this.memory = ByteBuffer.allocate((maxEntrySize + Bytes.INTEGER + Bytes.INTEGER) * 2);
    memory.limit(0);
    this.namespace = namespace;
    this.firstIndex = descriptor.index();
    reset(0);
  }

  /**
   * Initializes the writer by seeking to the end of the segment.
   */
  @SuppressWarnings("unchecked")
  private void reset(long index) {
    long nextIndex = firstIndex;

    // Clear the buffer indexes.
    try {
      channel.position(RaftLogSegmentDescriptor.BYTES);
      memory.clear().flip();

      // Record the current buffer position.
      long position = channel.position();

      // Read more bytes from the segment if necessary.
      if (memory.remaining() < maxEntrySize) {
        memory.clear();
        channel.read(memory);
        channel.position(position);
        memory.flip();
      }

      // Read the entry length.
      memory.mark();
      int length = memory.getInt();

      // If the length is non-zero, read the entry.
      while (0 < length && length <= maxEntrySize && (index == 0 || nextIndex <= index)) {

        // Read the checksum of the entry.
        final long checksum = memory.getInt() & 0xFFFFFFFFL;

        // Compute the checksum for the entry bytes.
        final Checksum crc32 = new CRC32();
        crc32.update(memory.array(), memory.position(), length);

        // If the stored checksum equals the computed checksum, return the entry.
        if (checksum == crc32.getValue()) {
          int limit = memory.limit();
          memory.limit(memory.position() + length);
          final E entry = namespace.deserialize(memory);
          memory.limit(limit);
          lastEntry = new Indexed<>(nextIndex, entry, length);
          this.index.index(nextIndex, (int) position);
          nextIndex++;
        } else {
          break;
        }

        // Update the current position for indexing.
        position = channel.position() + memory.position();

        // Read more bytes from the segment if necessary.
        if (memory.remaining() < maxEntrySize) {
          channel.position(position);
          memory.clear();
          channel.read(memory);
          channel.position(position);
          memory.flip();
        }

        memory.mark();
        length = memory.getInt();
      }

      // Reset the buffer to the previous mark.
      channel.position(channel.position() + memory.reset().position());
    } catch (IOException e) {
      throw new RaftIOException(e);
    }
  }

  @Override
  public long getLastIndex() {
    return lastEntry != null ? lastEntry.index() : descriptor.index() - 1;
  }

  @Override
  public Indexed<E> getLastEntry() {
    return lastEntry;
  }

  @Override
  public long getNextIndex() {
    if (lastEntry != null) {
      return lastEntry.index() + 1;
    } else {
      return firstIndex;
    }
  }

  /**
   * Returns the size of the underlying buffer.
   *
   * @return The size of the underlying buffer.
   */
  public long size() {
    try {
      return channel.position();
    } catch (IOException e) {
      throw new RaftIOException(e);
    }
  }

  /**
   * Returns a boolean indicating whether the segment is empty.
   *
   * @return Indicates whether the segment is empty.
   */
  public boolean isEmpty() {
    return lastEntry == null;
  }

  /**
   * Returns a boolean indicating whether the segment is full.
   *
   * @return Indicates whether the segment is full.
   */
  public boolean isFull() {
    return size() >= descriptor.maxSegmentSize()
        || getNextIndex() - firstIndex >= descriptor.maxEntries();
  }

  /**
   * Returns the first index written to the segment.
   */
  public long firstIndex() {
    return firstIndex;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void append(Indexed<E> entry) {
    final long nextIndex = getNextIndex();

    // If the entry's index is greater than the next index in the segment, skip some entries.
    if (entry.index() > nextIndex) {
      throw new IndexOutOfBoundsException("Entry index is not sequential");
    }

    // If the entry's index is less than the next index, truncate the segment.
    if (entry.index() < nextIndex) {
      truncate(entry.index() - 1);
    }
    append(entry.entry());
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends E> Indexed<T> append(T entry) {
    // Store the entry index.
    final long index = getNextIndex();

    try {
      // Serialize the entry.
      memory.clear();
      memory.position(Bytes.INTEGER + Bytes.INTEGER);
      namespace.serialize(entry, memory);
      memory.flip();
      final int length = memory.limit() - (Bytes.INTEGER + Bytes.INTEGER);

      // Ensure there's enough space left in the buffer to store the entry.
      long position = channel.position();
      if (descriptor.maxSegmentSize() - position < length + Bytes.INTEGER + Bytes.INTEGER) {
        throw new BufferOverflowException();
      }

      // If the entry length exceeds the maximum entry size then throw an exception.
      if (length > maxEntrySize) {
        throw new StorageException.TooLarge("Entry size " + length + " exceeds maximum allowed bytes (" + maxEntrySize + ")");
      }

      // Compute the checksum for the entry.
      final Checksum crc32 = new CRC32();
      crc32.update(memory.array(), Bytes.INTEGER + Bytes.INTEGER, memory.limit() - (Bytes.INTEGER + Bytes.INTEGER));
      final long checksum = crc32.getValue();

      // Create a single byte[] in memory for the entire entry and write it as a batch to the underlying buffer.
      memory.putInt(0, length);
      memory.putInt(Bytes.INTEGER, (int) checksum);
      channel.write(memory);

      // Update the last entry with the correct index/term/length.
      Indexed<E> indexedEntry = new Indexed<>(index, entry, length);
      this.lastEntry = indexedEntry;
      this.index.index(index, (int) position);
      return (Indexed<T>) indexedEntry;
    } catch (IOException e) {
      throw new RaftIOException(e);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public void truncate(long index) {
    // If the index is greater than or equal to the last index, skip the truncate.
    if (index >= getLastIndex()) {
      return;
    }

    // Reset the last entry.
    lastEntry = null;

    try {
      // Truncate the index.
      this.index.truncate(index);

      if (index < descriptor.index()) {
        channel.position(RaftLogSegmentDescriptor.BYTES);
        memory.clear();
        for (int i = 0; i < memory.limit(); i++) {
          memory.put(i, (byte) 0);
        }
        channel.write(memory);
        channel.position(RaftLogSegmentDescriptor.BYTES);
      } else {
        // Reset the writer to the given index.
        reset(index);

        // Zero entries after the given index.
        long position = channel.position();
        memory.clear();
        for (int i = 0; i < memory.limit(); i++) {
          memory.put(i, (byte) 0);
        }
        channel.write(memory);
        channel.position(position);
      }
    } catch (IOException e) {
      throw new RaftIOException(e);
    }
  }

  @Override
  public void flush() {
    try {
      channel.force(true);
    } catch (IOException e) {
      throw new RaftIOException(e);
    }
  }

  @Override
  public void close() {
    try {
      flush();
      channel.close();
    } catch (IOException e) {
      throw new RaftIOException(e);
    }
  }

  /**
   * Deletes the segment.
   */
  void delete() {
    try {
      Files.deleteIfExists(file.toPath());
    } catch (IOException e) {
      throw new RaftIOException(e);
    }
  }
}