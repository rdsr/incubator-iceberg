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

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.Collection;
import java.util.Set;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;

class IncrDataTableScan extends DataTableScan implements IncrTableScan {
  private final TableOperations ops;
  private final Table table;

  private Long snapshotId;
  private boolean colStats;
  private Collection<String> selectedColumns;

  private Long startSnapshotId;
  private Long endSnapshotId;

  IncrDataTableScan(TableOperations ops, Table table) {
    super(ops, table);
    this.ops = ops;
    this.table = table;
  }

  IncrDataTableScan(TableOperations ops, Table table, Long snapshotId, Schema schema,
                    Expression rowFilter, boolean caseSensitive, boolean colStats,
                    Collection<String> selectedColumns,
                    Long startSnapshotId, Long endSnapshotId) {
    super(ops, table, snapshotId, schema, rowFilter, caseSensitive, colStats, selectedColumns);

    this.ops = ops;
    this.table = table;
    this.snapshotId = snapshotId;
    this.colStats = colStats;
    this.selectedColumns = selectedColumns;

    this.startSnapshotId = startSnapshotId;
    this.endSnapshotId = endSnapshotId;
  }

  @Override
  public IncrTableScan readRange(Long newStartSnapshotId, Long newEndSnapshotId) {
    return new IncrDataTableScan(ops, table, snapshotId, schema(), filter(), isCaseSensitive(),
            colStats, selectedColumns, newStartSnapshotId, newEndSnapshotId);
  }

  @Override
  public CloseableIterable<FileScanTask> planFiles() {
    Snapshot s1 = table.snapshot(startSnapshotId);
    Snapshot s2 = table.snapshot(endSnapshotId);

    Set<DataFile> s1Files = files(s1);
    Set<DataFile> s2Files = files(s2);
    // Remove all those files which were already in s1
    Set<DataFile> files = Sets.difference(s2Files, s1Files);

    AppendFiles appendFiles = new IncrAppend(ops);
    for (DataFile f : files) {
      appendFiles.appendFile(f);
    }
    // TODO: cleanup temporary snapshots
    Snapshot snapshot = appendFiles.apply();
    return planFiles(ops, snapshot, filter(), isCaseSensitive(), colStats);
  }

  @Override
  @SuppressWarnings("checkstyle:HiddenField")
  protected TableScan newRefinedScan(
          TableOperations ops, Table table, Long snapshotId, Schema schema, Expression rowFilter,
          boolean caseSensitive, boolean colStats, Collection<String> selectedColumns) {
    return new IncrDataTableScan(
            ops, table, snapshotId, schema, rowFilter, caseSensitive, colStats, selectedColumns,
            startSnapshotId, endSnapshotId);
  }

  private Set<DataFile> files(Snapshot snapshot) {
    Set<DataFile> files = Sets.newHashSet();
    // TODO this can be optimised, by using Snapshot#getAddedFiles etc.
    for (String manifest : Iterables.transform(snapshot.manifests(), ManifestFile::path)) {
      try (ManifestReader reader = ManifestReader.read(
              ops.io().newInputFile(manifest),
              ops.current()::spec)) {
        for (ManifestEntry manifestEntry : reader.entries()) {
          ManifestEntry.Status status = manifestEntry.status();
          // Existing entry is only set when manifests are merged
          if (status == ManifestEntry.Status.ADDED || status == ManifestEntry.Status.EXISTING) {
            files.add(manifestEntry.file().copy());
          }
        }
      } catch (IOException e) {
        throw new RuntimeIOException(e, "Failed to close reader while caching changes");
      }
    }
    return files;
  }
}
