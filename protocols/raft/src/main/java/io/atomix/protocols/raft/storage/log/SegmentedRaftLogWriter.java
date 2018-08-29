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

import io.atomix.protocols.raft.storage.log.entry.RaftLogEntry;

import java.nio.BufferOverflowException;

/**
 * Raft log writer.
 */
public class SegmentedRaftLogWriter implements RaftLogWriter<RaftLogEntry> {
  private final SegmentedRaftLog log;
  private RaftLogSegment<RaftLogEntry> currentSegment;
  private RaftLogSegmentWriter<RaftLogEntry> currentWriter;

  public SegmentedRaftLogWriter(SegmentedRaftLog log) {
    this.log = log;
    this.currentSegment = log.getLastSegment();
    this.currentWriter = currentSegment.writer();
  }

  @Override
  public long getLastIndex() {
    return currentWriter.getLastIndex();
  }

  @Override
  public Indexed<RaftLogEntry> getLastEntry() {
    return currentWriter.getLastEntry();
  }

  @Override
  public long getNextIndex() {
    return currentWriter.getNextIndex();
  }

  /**
   * Resets the head of the journal to the given index.
   *
   * @param index the index to which to reset the head of the journal
   */
  public void reset(long index) {
    if (index > currentWriter.firstIndex()) {
      currentWriter.close();
      currentSegment = log.resetSegments(index);
      currentWriter = currentSegment.writer();
      log.resetHead(index);
    } else {
      truncate(index - 1);
    }
  }

  /**
   * Commits entries up to the given index.
   *
   * @param index The index up to which to commit entries.
   */
  public void commit(long index) {
    if (index > log.getCommitIndex()) {
      log.setCommitIndex(index);
      if (log.isFlushOnCommit()) {
        flush();
      }
    }
  }

  @Override
  public <T extends RaftLogEntry> Indexed<T> append(T entry) {
    try {
      if (currentWriter.isFull()) {
        currentWriter.flush();
        currentSegment = log.getNextSegment();
        currentWriter = currentSegment.writer();
      }
      return currentWriter.append(entry);
    } catch (BufferOverflowException e) {
      if (currentWriter.firstIndex() == currentWriter.getNextIndex()) {
        throw e;
      }
      currentWriter.flush();
      currentSegment = log.getNextSegment();
      currentWriter = currentSegment.writer();
      return currentWriter.append(entry);
    }
  }

  @Override
  public void append(Indexed<RaftLogEntry> entry) {
    try {
      if (currentWriter.isFull()) {
        currentWriter.flush();
        currentSegment = log.getNextSegment();
        currentWriter = currentSegment.writer();
      }
      currentWriter.append(entry);
    } catch (BufferOverflowException e) {
      if (currentWriter.firstIndex() == currentWriter.getNextIndex()) {
        throw e;
      }
      currentWriter.flush();
      currentSegment = log.getNextSegment();
      currentWriter = currentSegment.writer();
      currentWriter.append(entry);
    }
  }

  @Override
  public void truncate(long index) {
    if (index < log.getCommitIndex()) {
      throw new IndexOutOfBoundsException("Cannot truncate committed index: " + index);
    }

    // Delete all segments with first indexes greater than the given index.
    while (index < currentWriter.firstIndex() - 1) {
      currentWriter.close();
      log.removeSegment(currentSegment);
      currentSegment = log.getLastSegment();
      currentWriter = currentSegment.writer();
    }

    // Truncate the current index.
    currentWriter.truncate(index);

    // Reset segment readers.
    log.resetTail(index + 1);
  }

  @Override
  public void flush() {
    currentWriter.flush();
  }

  @Override
  public void close() {
    currentWriter.close();
  }
}
