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
import io.atomix.protocols.raft.storage.log.index.SparseRaftLogIndex;
import io.atomix.utils.serializer.Namespace;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;

/**
 * Log segment.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class RaftLogSegment<E> implements AutoCloseable {
  private final RaftLogSegmentFile file;
  private final RaftLogSegmentDescriptor descriptor;
  private final int maxEntrySize;
  private final RaftLogIndex index;
  private final Namespace namespace;
  private final RaftLogSegmentWriter<E> writer;
  private final RaftLogSegmentCache cache;
  private boolean open = true;

  public RaftLogSegment(
      RaftLogSegmentFile file,
      RaftLogSegmentDescriptor descriptor,
      int maxEntrySize,
      double indexDensity,
      int cacheSize,
      Namespace namespace) {
    this.file = file;
    this.descriptor = descriptor;
    this.maxEntrySize = maxEntrySize;
    this.index = new SparseRaftLogIndex(indexDensity);
    this.namespace = namespace;
    this.cache = new RaftLogSegmentCache(descriptor.index(), cacheSize);
    this.writer = new RaftLogSegmentWriter<>(file.file(), openChannel(file.file()), descriptor, maxEntrySize, cache, index, namespace);
  }

  private FileChannel openChannel(File file) {
    try {
      if (file.exists()) {
        return FileChannel.open(file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
      } else {
        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        raf.setLength(descriptor.maxSegmentSize());
        return raf.getChannel();
      }
    } catch (IOException e) {
      throw new RaftIOException(e);
    }
  }

  /**
   * Returns the segment ID.
   *
   * @return The segment ID.
   */
  public long id() {
    return descriptor.id();
  }

  /**
   * Returns the segment version.
   *
   * @return The segment version.
   */
  public long version() {
    return descriptor.version();
  }

  /**
   * Returns the segment's starting index.
   *
   * @return The segment's starting index.
   */
  public long index() {
    return descriptor.index();
  }

  /**
   * Returns the last index in the segment.
   *
   * @return The last index in the segment.
   */
  public long lastIndex() {
    return writer.getLastIndex();
  }

  /**
   * Returns the segment file.
   *
   * @return The segment file.
   */
  public RaftLogSegmentFile file() {
    return file;
  }

  /**
   * Returns the segment descriptor.
   *
   * @return The segment descriptor.
   */
  public RaftLogSegmentDescriptor descriptor() {
    return descriptor;
  }

  /**
   * Returns the segment size.
   *
   * @return The segment size.
   */
  public long size() {
    return writer.size();
  }

  /**
   * Returns a boolean value indicating whether the segment is empty.
   *
   * @return Indicates whether the segment is empty.
   */
  public boolean isEmpty() {
    return length() == 0;
  }

  /**
   * Returns a boolean indicating whether the segment is full.
   *
   * @return Indicates whether the segment is full.
   */
  public boolean isFull() {
    return writer.isFull();
  }

  /**
   * Returns the segment length.
   *
   * @return The segment length.
   */
  public long length() {
    return writer.getNextIndex() - index();
  }

  /**
   * Returns the segment writer.
   *
   * @return The segment writer.
   */
  public RaftLogSegmentWriter<E> writer() {
    checkOpen();
    return writer;
  }

  /**
   * Creates a new segment reader.
   *
   * @return A new segment reader.
   */
  RaftLogSegmentReader<E> createReader() {
    checkOpen();
    return new RaftLogSegmentReader<>(openChannel(file.file()), descriptor, maxEntrySize, cache, index, namespace);
  }

  /**
   * Checks whether the segment is open.
   */
  private void checkOpen() {
    checkState(open, "Segment not open");
  }

  /**
   * Returns a boolean indicating whether the segment is open.
   *
   * @return indicates whether the segment is open
   */
  public boolean isOpen() {
    return open;
  }

  /**
   * Closes the segment.
   */
  @Override
  public void close() {
    writer.close();
    descriptor.close();
    open = false;
  }

  /**
   * Deletes the segment.
   */
  public void delete() {
    writer.delete();
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("id", id())
        .add("version", version())
        .add("index", index())
        .add("size", size())
        .toString();
  }
}