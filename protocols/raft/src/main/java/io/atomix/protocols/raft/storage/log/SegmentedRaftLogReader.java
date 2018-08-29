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

import java.util.NoSuchElementException;

/**
 * Raft log reader.
 */
public class SegmentedRaftLogReader implements RaftLogReader<RaftLogEntry> {

  /**
   * Raft log reader mode.
   */
  public enum Mode {

    /**
     * Reads all entries from the log.
     */
    ALL,

    /**
     * Reads committed entries from the log.
     */
    COMMITS,
  }

  private final SegmentedRaftLog log;
  private RaftLogSegment<RaftLogEntry> currentSegment;
  private Indexed<RaftLogEntry> previousEntry;
  private RaftLogSegmentReader<RaftLogEntry> currentReader;
  private final Mode mode;

  public SegmentedRaftLogReader(SegmentedRaftLog log, long index, Mode mode) {
    this.log = log;
    this.mode = mode;
    initialize(index);
  }

  /**
   * Initializes the reader to the given index.
   */
  private void initialize(long index) {
    currentSegment = log.getSegment(index);
    currentReader = currentSegment.createReader();
    long nextIndex = getNextIndex();
    while (index > nextIndex && hasNext()) {
      next();
      nextIndex = getNextIndex();
    }
  }

  /**
   * Returns the first index in the journal.
   *
   * @return the first index in the journal
   */
  public long getFirstIndex() {
    return log.getFirstSegment().index();
  }

  @Override
  public long getCurrentIndex() {
    long currentIndex = currentReader.getCurrentIndex();
    if (currentIndex != 0) {
      return currentIndex;
    }
    if (previousEntry != null) {
      return previousEntry.index();
    }
    return 0;
  }

  @Override
  public Indexed<RaftLogEntry> getCurrentEntry() {
    Indexed<RaftLogEntry> currentEntry = currentReader.getCurrentEntry();
    if (currentEntry != null) {
      return currentEntry;
    }
    return previousEntry;
  }

  @Override
  public long getNextIndex() {
    return currentReader.getNextIndex();
  }

  @Override
  public void reset() {
    currentReader.close();
    currentSegment = log.getFirstSegment();
    currentReader = currentSegment.createReader();
    previousEntry = null;
  }

  @Override
  public void reset(long index) {
    // If the current segment is not open, it has been replaced. Reset the segments.
    if (!currentSegment.isOpen()) {
      reset();
    }

    if (index < currentReader.getNextIndex()) {
      rewind(index);
    } else if (index > currentReader.getNextIndex()) {
      forward(index);
    } else {
      currentReader.reset(index);
    }
  }

  /**
   * Rewinds the journal to the given index.
   */
  private void rewind(long index) {
    if (currentSegment.index() >= index) {
      RaftLogSegment<RaftLogEntry> segment = log.getSegment(index - 1);
      if (segment != null) {
        currentReader.close();
        currentSegment = segment;
        currentReader = currentSegment.createReader();
      }
    }

    currentReader.reset(index);
    previousEntry = currentReader.getCurrentEntry();
  }

  /**
   * Fast forwards the journal to the given index.
   */
  private void forward(long index) {
    while (getNextIndex() < index && hasNext()) {
      next();
    }
  }

  @Override
  public boolean hasNext() {
    if (mode == Mode.ALL) {
      return hasNextEntry();
    }

    long nextIndex = getNextIndex();
    long commitIndex = log.getCommitIndex();
    return nextIndex <= commitIndex && hasNextEntry();
  }

  private boolean hasNextEntry() {
    if (!currentReader.hasNext()) {
      RaftLogSegment<RaftLogEntry> nextSegment = log.getNextSegment(currentSegment.index());
      if (nextSegment != null && nextSegment.index() == getNextIndex()) {
        previousEntry = currentReader.getCurrentEntry();
        currentSegment = nextSegment;
        currentReader = currentSegment.createReader();
        return currentReader.hasNext();
      }
      return false;
    }
    return true;
  }

  @Override
  public Indexed<RaftLogEntry> next() {
    if (!currentReader.hasNext()) {
      RaftLogSegment<RaftLogEntry> nextSegment = log.getNextSegment(currentSegment.index());
      if (nextSegment != null && nextSegment.index() == getNextIndex()) {
        previousEntry = currentReader.getCurrentEntry();
        currentSegment = nextSegment;
        currentReader = currentSegment.createReader();
        return currentReader.next();
      } else {
        throw new NoSuchElementException();
      }
    } else {
      previousEntry = currentReader.getCurrentEntry();
      return currentReader.next();
    }
  }

  @Override
  public void close() {
    currentReader.close();
    log.closeReader(this);
  }
}
