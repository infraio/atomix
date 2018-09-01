/*
 * Copyright 2018-present Open Networking Foundation
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
import io.atomix.utils.serializer.Namespace;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Mappable log segment writer.
 */
class MappableLogSegmentWriter<E> implements RaftLogWriter<E> {
  private final FileChannel channel;
  private final RaftLogSegment segment;
  private final int maxEntrySize;
  private final RaftLogIndex index;
  private final Namespace namespace;
  private RaftLogWriter<E> writer;

  MappableLogSegmentWriter(
      FileChannel channel,
      RaftLogSegment segment,
      int maxEntrySize,
      RaftLogIndex index,
      Namespace namespace) {
    this.channel = channel;
    this.segment = segment;
    this.maxEntrySize = maxEntrySize;
    this.index = index;
    this.namespace = namespace;
    this.writer = new FileChannelLogSegmentWriter<>(channel, segment, maxEntrySize, index, namespace);
  }

  /**
   * Maps the segment writer into memory, returning the mapped buffer.
   *
   * @return the buffer that was mapped into memory
   */
  MappedByteBuffer map() {
    if (writer instanceof MappedLogSegmentWriter) {
      return ((MappedLogSegmentWriter<E>) writer).buffer();
    }

    try {
      RaftLogWriter<E> writer = this.writer;
      MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, segment.descriptor().maxSegmentSize());
      this.writer = new MappedLogSegmentWriter<>(buffer, segment, maxEntrySize, index, namespace);
      writer.close();
      return buffer;
    } catch (IOException e) {
      throw new RaftIOException(e);
    }
  }

  /**
   * Unmaps the mapped buffer.
   */
  void unmap() {
    if (writer instanceof MappedLogSegmentWriter) {
      RaftLogWriter<E> writer = this.writer;
      this.writer = new FileChannelLogSegmentWriter<>(channel, segment, maxEntrySize, index, namespace);
      writer.close();
    }
  }

  MappedByteBuffer buffer() {
    RaftLogWriter<E> writer = this.writer;
    if (writer instanceof MappedLogSegmentWriter) {
      return ((MappedLogSegmentWriter<E>) writer).buffer();
    }
    return null;
  }

  /**
   * Returns the writer's first index.
   *
   * @return the writer's first index
   */
  public long firstIndex() {
    return segment.index();
  }

  @Override
  public long getLastIndex() {
    return writer.getLastIndex();
  }

  @Override
  public Indexed<E> getLastEntry() {
    return writer.getLastEntry();
  }

  @Override
  public long getNextIndex() {
    return writer.getNextIndex();
  }

  @Override
  public <T extends E> Indexed<T> append(T entry) {
    return writer.append(entry);
  }

  @Override
  public void append(Indexed<E> entry) {
    writer.append(entry);
  }

  @Override
  public void truncate(long index) {
    writer.truncate(index);
  }

  @Override
  public void flush() {
    writer.flush();
  }

  @Override
  public void close() {
    writer.close();
    try {
      channel.close();
    } catch (IOException e) {
      throw new RaftIOException(e);
    }
  }
}
