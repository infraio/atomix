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

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Mappable log segment reader.
 */
public class MappableLogSegmentReader<E> implements RaftLogReader<E> {
  private final FileChannel channel;
  private final RaftLogSegmentDescriptor descriptor;
  private final int maxEntrySize;
  private final RaftLogSegmentCache cache;
  private final RaftLogIndex index;
  private final Namespace namespace;
  private RaftLogReader<E> reader;

  public MappableLogSegmentReader(
      FileChannel channel,
      RaftLogSegmentDescriptor descriptor,
      int maxEntrySize,
      RaftLogSegmentCache cache,
      RaftLogIndex index,
      Namespace namespace) {
    this.channel = channel;
    this.descriptor = descriptor;
    this.maxEntrySize = maxEntrySize;
    this.cache = cache;
    this.index = index;
    this.namespace = namespace;
    this.reader = new FileChannelLogSegmentReader<>(channel, descriptor, maxEntrySize, cache, index, namespace);
  }

  void map(ByteBuffer buffer) {
    this.reader = new MappedLogSegmentReader<>(buffer, descriptor, maxEntrySize, index, namespace);
  }

  void unmap() {
    this.reader = new FileChannelLogSegmentReader<>(channel, descriptor, maxEntrySize, cache, index, namespace);
  }

  @Override
  public long getCurrentIndex() {
    return reader.getCurrentIndex();
  }

  @Override
  public Indexed<E> getCurrentEntry() {
    return reader.getCurrentEntry();
  }

  @Override
  public long getNextIndex() {
    return reader.getNextIndex();
  }

  @Override
  public boolean hasNext() {
    return reader.hasNext();
  }

  @Override
  public Indexed<E> next() {
    return reader.next();
  }

  @Override
  public void reset() {
    reader.reset();
  }

  @Override
  public void reset(long index) {
    reader.reset(index);
  }

  @Override
  public void close() {
    reader.close();
  }
}
