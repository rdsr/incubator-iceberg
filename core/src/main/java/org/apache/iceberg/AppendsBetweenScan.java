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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.util.SnapshotUtil;

class AppendsBetweenScan extends DataTableScan implements TableScan {
  private final TableOperations ops;
  private final Table table;
  private boolean colStats;
  private long fromSnapshotId;
  private long toSnapshotId;

  AppendsBetweenScan(TableOperations ops, Table table, long fromSnapshotId, long toSnapshotId) {
    super(ops, table);
    this.ops = ops;
    this.table = table;
    this.fromSnapshotId = fromSnapshotId;
    this.toSnapshotId = toSnapshotId;
  }

  /**
   * @return all snapshots ids between {@param fromSnapshotId } exclusive
   * and {@param toSnapshotId} inclusive
   */
  private static List<Snapshot> snapshotsWithin(Table table, long fromSnapshotId, long toSnapshotId) {
    List<Long> snapshotIds = SnapshotUtil.currentAncestors(table);
    int toSnapshotIdIndex = snapshotIds.indexOf(toSnapshotId);
    Preconditions.checkArgument(toSnapshotIdIndex != -1, "toSnapshotId: {} does not exist", toSnapshotId);
    int fromSnapshotIdIndex = snapshotIds.indexOf(fromSnapshotId);
    Preconditions.checkArgument(fromSnapshotIdIndex != -1, "fromSnapshotId: {} does not exist", fromSnapshotId);

    List<Snapshot> snapshots = Lists.newArrayList();
    for (int i = toSnapshotIdIndex; i < fromSnapshotIdIndex; i++) {
      Snapshot snapshot = table.snapshot(snapshotIds.get(i));
      if (snapshot.operation().equals(DataOperations.APPEND)) {
        snapshots.add(snapshot);
      }
    }
    return snapshots;
  }

  private AppendsBetweenScan(TableOperations ops, Table table, Long snapshotId, Schema schema,
                             Expression rowFilter, boolean caseSensitive, boolean colStats,
                             Collection<String> selectedColumns, ImmutableMap<String, String> options,
                             Long fromSnapshotId, Long toSnapshotId) {
    super(ops, table, snapshotId, schema, rowFilter, caseSensitive, colStats, selectedColumns, options);
    this.ops = ops;
    this.table = table;
    this.colStats = colStats;
    this.fromSnapshotId = fromSnapshotId;
    this.toSnapshotId = toSnapshotId;
  }

  @Override
  public CloseableIterable<FileScanTask> planFiles() {
    //TODO publish an incremental appends scan event
    List<Snapshot> snapshots = snapshotsWithin(table, fromSnapshotId, toSnapshotId);
    final Iterable<CloseableIterable<FileScanTask>> files = Iterables.transform(snapshots, this::planFiles);
    return CloseableIterable.concat(files);
  }

  public CloseableIterable<FileScanTask> planFiles(Snapshot snapshot) {
    Predicate<ManifestFile> matchingManifests = manifest ->
        Objects.equals(manifest.snapshotId(), snapshot.snapshotId());

    Predicate<ManifestEntry> matchingManifestEntries =
        manifestEntry ->
            manifestEntry.snapshotId() == snapshot.snapshotId() &&
            manifestEntry.status() == ManifestEntry.Status.ADDED;

    return planFiles(ops, snapshot, filter(), isCaseSensitive(), colStats, matchingManifests, matchingManifestEntries);
  }

  @Override
  @SuppressWarnings("checkstyle:HiddenField")
  protected TableScan newRefinedScan(
          TableOperations ops, Table table, Long snapshotId, Schema schema, Expression rowFilter,
          boolean caseSensitive, boolean colStats, Collection<String> selectedColumns,
          ImmutableMap<String, String> options) {
    return new AppendsBetweenScan(
            ops, table, snapshotId, schema, rowFilter, caseSensitive, colStats, selectedColumns, options,
            fromSnapshotId, toSnapshotId);
  }
}
