package org.apache.iceberg.mr;

import com.google.common.collect.Sets;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.v2.MiniMRYarnCluster;
import org.apache.iceberg.data.GenericRecord;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestMRJob {
  private static Path OUTPUT_PATH;
  private static MiniMRYarnCluster MR_CLUSTER;
  private static MiniDFSCluster DFS_CLUSTER;

  @BeforeClass
  public static void setupMiniCluster() throws Exception {
    OUTPUT_PATH = new Path("s3://bucket-name/output/path");
    Configuration conf = new Configuration();
    DFS_CLUSTER = new MiniDFSCluster.Builder(conf).build();
    MR_CLUSTER = new MiniMRYarnCluster("test-Iceberg-MR-job", 2);
    MR_CLUSTER.init(DFS_CLUSTER.getConfiguration(0));
    MR_CLUSTER.start();
  }

  @AfterClass
  public static void stopMiniMRCluster() {
    if (MR_CLUSTER != null) {
      MR_CLUSTER.stop();
    }
    if (DFS_CLUSTER != null) {
      DFS_CLUSTER.shutdown();
    }
  }


  public static class Map extends Mapper<Void, GenericRecord, Void, Text> {
    @Override
    protected void map(Void key, GenericRecord value, Context context) throws IOException, InterruptedException {
      context.write(key, new Text(value.toString()));
    }
  }

  @Rule
  public final TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void testMRJob() throws Exception {
    /*
    String commitUUID = UUID.randomUUID().toString();

    int numFiles = 3;
    Set<String> expectedFiles = Sets.newHashSet();
    for (int i = 0; i < numFiles; i += 1) {
      File file = temp.newFile(String.valueOf(i) + ".text");
      try (FileOutputStream out = new FileOutputStream(file)) {
        out.write(("file " + i).getBytes(StandardCharsets.UTF_8));
      }
      expectedFiles.add(new Path(
          OUTPUT_PATH, "part-m-0000" + i + "-" + commitUUID).toString());
    }

    Job mrJob = Job.getInstance(MR_CLUSTER.getConfig(), "test-committer-job");
    Configuration conf = mrJob.getConfiguration();

    S3TextOutputFormat.setOutputPath(mrJob, OUTPUT_PATH);

    File mockResultsFile = temp.newFile("committer.bin");
    mockResultsFile.delete();
    String committerPath = "file:" + mockResultsFile;
    conf.set("mock-results-file", committerPath);

    mrJob.setInputFormatClass(TextInputFormat.class);
    TextInputFormat.addInputPath(mrJob, new Path("file:" + temp.getRoot().toString()));

    mrJob.setMapperClass(Map.class);
    mrJob.setNumReduceTasks(0);

    mrJob.submit();
    Assert.assertTrue("MR job should succeed", mrJob.waitForCompletion(true));
     */
  }
}