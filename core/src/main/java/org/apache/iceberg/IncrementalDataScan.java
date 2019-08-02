/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Sets;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;

class IncrementalDataScan extends DataTableScan implements TableScan {
  private final TableOperations ops;
  private final Table table;
  private boolean colStats;
  private long fromSnapshotId;
  private long toSnapshotId;
  private Set<Long> snapshotIds;


  IncrementalDataScan(TableOperations ops, Table table, long fromSnapshotId, long toSnapshotId) {
    super(ops, table);
    this.ops = ops;
    this.table = table;
    this.fromSnapshotId = fromSnapshotId;
    this.toSnapshotId = toSnapshotId;
    this.snapshotIds = snapshotsWithin(table, fromSnapshotId, toSnapshotId);
  }

  /**
   * @return all snapshots ids between {@param fromSnapshotId } exclusive
   * and {@param toSnapshotId} inclusive
   */
  private static Set<Long> snapshotsWithin(Table table, long fromSnapshotId, long toSnapshotId) {
    Set<Long> snapshotIds = Sets.newHashSet();
    // table history is ordered
    Iterator<HistoryEntry> i = table.history().iterator();
    while (i.hasNext()) {
      if (i.next().snapshotId() == fromSnapshotId) {
        break;
      }
    }
    while (i.hasNext()) {
      long snapshotId = i.next().snapshotId();
      snapshotIds.add(snapshotId);
      if (snapshotId == toSnapshotId) {
        break;
      }
    }
    return snapshotIds;
  }

  private IncrementalDataScan(TableOperations ops, Table table, Long snapshotId, Schema schema,
                              Expression rowFilter, boolean caseSensitive, boolean colStats,
                              Collection<String> selectedColumns,
                              Long fromSnapshotId, Long toSnapshotId) {
    super(ops, table, snapshotId, schema, rowFilter, caseSensitive, colStats, selectedColumns);
    this.ops = ops;
    this.table = table;
    this.colStats = colStats;
    this.fromSnapshotId = fromSnapshotId;
    this.toSnapshotId = toSnapshotId;
  }


  @Override
  public CloseableIterable<FileScanTask> planFiles() {
    Snapshot snapshot = table.snapshot(toSnapshotId);
    Preconditions.checkState(snapshot != null, "Cannot find snapshot corresponding to snapshot id: %s", toSnapshotId);
    //TODO publish an incremental scan event
    return planFiles(ops, snapshot, filter(), isCaseSensitive(), colStats);
  }

  @Override
  public CloseableIterable<FileScanTask> planFiles(
          TableOperations ops, Snapshot snapshot, Expression rowFilter, boolean caseSensitive, boolean colStats) {
    // We only consider manifests belonging to (fromSnapshotId, toSnapshotId]
    // Invariant is that for snapshot 'toSnapshotId' a manifest not belonging
    // to a snapshot id in (fromSnapshotId, toSnapshotId] will never have manifest
    // entries belonging to (fromSnapshotId, toSnapshotId]
    Predicate<ManifestFile> matchingManifests =
            manifest -> snapshotIds.contains(manifest.snapshotId());
    // We only consider manifestEntries belonging to (fromSnapshotId, toSnapshotId]
    Predicate<ManifestEntry> matchingManifestEntries =
            manifestEntry -> snapshotIds.contains(manifestEntry.snapshotId());

    return planFiles(ops, snapshot, rowFilter, caseSensitive, colStats,
            matchingManifests, matchingManifestEntries);
  }

  @Override
  @SuppressWarnings("checkstyle:HiddenField")
  protected TableScan newRefinedScan(
          TableOperations ops, Table table, Long snapshotId, Schema schema, Expression rowFilter,
          boolean caseSensitive, boolean colStats, Collection<String> selectedColumns) {
    return new IncrementalDataScan(
            ops, table, snapshotId, schema, rowFilter, caseSensitive, colStats, selectedColumns,
            fromSnapshotId, toSnapshotId);
  }
}
