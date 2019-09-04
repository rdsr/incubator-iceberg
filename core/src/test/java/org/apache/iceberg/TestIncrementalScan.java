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
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.List;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

public class TestIncrementalScan extends TableTestBase {
  @Test
  public void testAppend() {
    // ManifestEntry.Existing flag is only set when manifests are merged
    table.updateProperties().set(TableProperties.MANIFEST_MIN_MERGE_COUNT, "2").commit();
    add(files("A")); // 0
    add(files("B"));
    add(files("C"));
    add(files("D"));
    add(files("E")); // 4
    Assert.assertEquals(Sets.newHashSet("B", "C", "D", "E"), appendsBetweenScan(0, 4));

    delete(files("C", "D", "E")); // 5
    Assert.assertEquals(Sets.newHashSet("B", "C", "D", "E"), appendsBetweenScan(0, 4));
    // Sine 5th is a snapshot with delete operation. It is ignored
    Assert.assertEquals(Sets.newHashSet("B", "C", "D", "E"), appendsBetweenScan(0, 5));
    Assert.assertEquals(Sets.newHashSet("D", "E"), appendsBetweenScan(2, 5));
    Assert.assertEquals(Sets.newHashSet("E"), appendsBetweenScan(3, 5));
    // Sine 5th is a snapshot with delete operation. It is ignored
    Assert.assertTrue(appendsBetweenScan(4, 5).isEmpty());

    add(files("F")); // 6
    add(files("G")); // 7
    add(files("H")); // 8

    // Idempotent scans - old identifiers still give back existing data
    Assert.assertEquals(Sets.newHashSet("B", "C", "D", "E"), appendsBetweenScan(0, 4));
    Assert.assertEquals(Sets.newHashSet("B", "C", "D", "E"), appendsBetweenScan(0, 5));

    Assert.assertEquals(Sets.newHashSet("B", "C", "D", "E", "F", "G", "H"), appendsBetweenScan(0, 8));
    Assert.assertEquals(Sets.newHashSet("B", "C", "D", "E", "F", "G"), appendsBetweenScan(0, 7));
    Assert.assertEquals(Sets.newHashSet("B", "C", "D", "E", "F"), appendsBetweenScan(0, 6));
  }

  @Test
  public void testReplace() {
    table.updateProperties().set(TableProperties.MANIFEST_MIN_MERGE_COUNT, "2").commit();
    add(files("A")); // 0
    add(files("B"));
    add(files("C"));
    add(files("D"));
    add(files("E")); // 4
    Assert.assertEquals(Sets.newHashSet("B", "C", "D", "E"), appendsBetweenScan(0, 4));

    replace(files("A", "B", "C"), files("F", "G")); // 5
    Assert.assertEquals(Sets.newHashSet("B", "C", "D", "E"), appendsBetweenScan(0, 5));
    // 5th snapshot was a replace. No new content was added
    Assert.assertTrue(appendsBetweenScan(4, 5).isEmpty());
    Assert.assertEquals(Sets.newHashSet("E"), appendsBetweenScan(3, 5));

    add(files("H"));
    add(files("I")); // 7
    Assert.assertEquals(Sets.newHashSet("B", "C", "D", "E", "H", "I"), appendsBetweenScan(0, 7));
    Assert.assertEquals(Sets.newHashSet("I"), appendsBetweenScan(6, 7));
    Assert.assertEquals(Sets.newHashSet("H", "I"), appendsBetweenScan(5, 7));
  }

  private static DataFile file(String name) {
    return DataFiles.builder(SPEC)
            .withPath(name + ".parquet")
            .withFileSizeInBytes(0)
            .withPartitionPath("data_bucket=0") // easy way to set partition data for now
            .withRecordCount(0)
            .build();
  }

  private void add(List<DataFile> adds) {
    AppendFiles appendFiles = table.newAppend();
    for (DataFile f : adds) {
      appendFiles.appendFile(f);
    }
    appendFiles.commit();
  }

  private void delete(List<DataFile> deletes) {
    DeleteFiles deleteFiles = table.newDelete();
    for (DataFile f : deletes) {
      deleteFiles.deleteFile(f);
    }
    deleteFiles.commit();
  }


  private void replace(List<DataFile> deletes, List<DataFile> adds) {
    RewriteFiles rewriteFiles = table.newRewrite();
    rewriteFiles.rewriteFiles(Sets.newHashSet(deletes), Sets.newHashSet(adds));
    rewriteFiles.commit();
  }

  private List<DataFile> files(String... names) {
    return Lists.transform(Lists.newArrayList(names), TestIncrementalScan::file);
  }

  private Set<String> appendsBetweenScan(int fromSnapshotIndex, int toSnapshotIndex) {
    List<Snapshot> snapshots = Lists.newArrayList(table.snapshots());
    Snapshot s1 = snapshots.get(fromSnapshotIndex);
    Snapshot s2 = snapshots.get(toSnapshotIndex);
    TableScan appendsBetween = table.newAppendsBetween(s1.snapshotId(), s2.snapshotId());
    Iterable<String> filesToRead = Iterables.transform(appendsBetween.planFiles(), t -> {
      String path = t.file().path().toString();
      return path.split("\\.")[0];
    });
    return Sets.newHashSet(filesToRead);
  }
}
