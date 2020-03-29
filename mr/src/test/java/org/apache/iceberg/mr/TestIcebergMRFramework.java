package org.apache.iceberg.mr;
/*
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.UUID;

import static org.mockito.Mockito.mock;

public class TestMRJob {

  private static Path S3_OUTPUT_PATH = null;
  private static MiniMRYarnCluster MR_CLUSTER = null;

  @BeforeClass
  public static void setupMiniMRCluster() {
    getConfiguration().set("fs.s3.impl", MockS3FileSystem.class.getName());
    S3_OUTPUT_PATH = new Path("s3://bucket-name/output/path");
    MR_CLUSTER = new MiniMRYarnCluster(
        "test-s3-multipart-output-committer", 2);
    MR_CLUSTER.init(getConfiguration());
    MR_CLUSTER.start();
  }

  @AfterClass
  public static void stopMiniMRCluster() {
    if (MR_CLUSTER != null) {
      MR_CLUSTER.stop();
    }
    MR_CLUSTER = null;
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
    String commitUUID = UUID.randomUUID().toString();

    int numFiles = 3;
    Set<String> expectedFiles = Sets.newHashSet();
    for (int i = 0; i < numFiles; i += 1) {
      File file = temp.newFile(String.valueOf(i) + ".text");
      try (FileOutputStream out = new FileOutputStream(file)) {
        out.write(("file " + i).getBytes(StandardCharsets.UTF_8));
      }
      expectedFiles.add(new Path(
          S3_OUTPUT_PATH, "part-m-0000" + i + "-" + commitUUID).toString());
    }

    Job mrJob = Job.getInstance(MR_CLUSTER.getConfig(), "test-committer-job");
    Configuration conf = mrJob.getConfiguration();

    S3TextOutputFormat.setOutputPath(mrJob, S3_OUTPUT_PATH);

    File mockResultsFile = temp.newFile("committer.bin");
    mockResultsFile.delete();
    String committerPath = "file:" + mockResultsFile;
    conf.set("mock-results-file", committerPath);

    mrJob.setInputFormatClass(TextInputFormat.class);
    TextInputFormat.addInputPath(mrJob,
                                 new Path("file:" + temp.getRoot().toString()));

    mrJob.setMapperClass(Map.class);
    mrJob.setNumReduceTasks(0);

    mrJob.submit();
    Assert.assertTrue("MR job should succeed", mrJob.waitForCompletion(true));

    TestUtil.ClientResults results;
    try (ObjectInputStream in = new ObjectInputStream(
        FileSystem.getLocal(conf).open(new Path(committerPath)))) {
      results = (TestUtil.ClientResults) in.readObject();
    }

    Assert.assertEquals("Should not delete files",
                        0, results.deletes.size());

    Assert.assertEquals("Should not abort commits",
                        0, results.aborts.size());

    Assert.assertEquals("Should commit task output files",
                        numFiles, results.commits.size());

    Set<String> actualFiles = Sets.newHashSet();
    for (CompleteMultipartUploadRequest commit : results.commits) {
      actualFiles.add("s3://" + commit.getBucketName() + "/" + commit.getKey());
    }

    Assert.assertEquals("Should commit the correct file paths",
                        expectedFiles, actualFiles);
  }

}*/