package org.apache.iceberg.mr;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.v2.MiniMRYarnCluster;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.mr.mapreduce.IcebergInputFormat;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.types.Types.NestedField.required;

@RunWith(Parameterized.class)
public class TestMRJob {
  private static Path OUTPUT_PATH;
  private static MiniMRYarnCluster MR_CLUSTER;
  private static MiniDFSCluster DFS_CLUSTER;
  private static Configuration conf;
  private static HadoopFileIO fileIO;

  static final Schema SCHEMA = new Schema(
      required(1, "data", Types.StringType.get()),
      required(3, "id", Types.LongType.get()),
      required(2, "date", Types.StringType.get()));

  static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA)
                                                 .identity("date")
                                                 .bucket("id", 1)
                                                 .build();

  @Parameterized.Parameters
  public static Object[][] parameters() {
    return new Object[][]{
        new Object[]{"parquet"},
        new Object[]{"avro"}
    };
  }

  private final FileFormat format;

  public TestMRJob(String format) {
    this.format = FileFormat.valueOf(format.toUpperCase(Locale.ENGLISH));
  }

  @BeforeClass
  public static void setupMiniCluster() throws Exception {

    Configuration conf = new Configuration();
    DFS_CLUSTER = new MiniDFSCluster.Builder(conf).build();
    MR_CLUSTER = new MiniMRYarnCluster("test-Iceberg-MR-job", 2);
    fileIO = new HadoopFileIO(conf);
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
    HadoopTables tables = new HadoopTables(conf);
    String prefix = "/tmp/iceberg-mr-job";
    String location = prefix + "/table";
    String output = prefix + "/output";
    FileSystem fs = FileSystem.get(conf);
    fs.delete(new Path(location), true);
    fs.delete(new Path(output), true);

    Table table = tables.create(SCHEMA, SPEC,
                                ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, format.name()),
                                location);
    List<Record> expectedRecords = RandomGenericData.generate(table.schema(), 1, 0L);
    expectedRecords.get(0).set(2, "2020-03-20");
    DataFile dataFile = writeFile(table, location, TestHelpers.Row.of("2020-03-20", 0), expectedRecords);
    table.newAppend()
         .appendFile(dataFile)
         .commit();

    //writeExpectedRecords(output, expectedRecords);

    Job job = Job.getInstance(MR_CLUSTER.getConfig(), "test-mr-job");
    IcebergInputFormat.ConfigBuilder configBuilder = IcebergInputFormat.configure(job);
    configBuilder.readFrom(location);
    Configuration conf = job.getConfiguration();
    mrJob.setMapperClass(Map.class);
    mrJob.setNumReduceTasks(0);

    mrJob.submit();
    Assert.assertTrue("MR job should succeed", mrJob.waitForCompletion(true));
     */
  }

  private DataFile writeFile(
      Table table, String path, StructLike partitionData, List<Record> records) throws IOException {
    FileAppender<Record> appender;
    switch (format) {
      case AVRO:
        appender = Avro.write(fileIO.newOutputFile(path + "/input-data" + format.name()))
                       .schema(table.schema())
                       .createWriterFunc(DataWriter::create)
                       .named(format.name())
                       .build();
        break;
      case PARQUET:
        appender = Parquet.write(fileIO.newOutputFile(path + "/input-data" + format.name()))
                          .schema(table.schema())
                          .createWriterFunc(GenericParquetWriter::buildWriter)
                          .named(format.name())
                          .build();
        break;
      default:
        throw new UnsupportedOperationException("Cannot write format: " + format);
    }

    try {
      appender.addAll(records);
    } finally {
      appender.close();
    }

    FileSystem fs = FileSystem.get(conf);
    ContentSummary contentSummary = fs.getContentSummary(new Path(path));
    final DataFiles.Builder builder = DataFiles.builder(table.spec())
                                               .withPath(path)
                                               .withFormat(format)
                                               .withFileSizeInBytes(contentSummary.getLength())
                                               .withMetrics(appender.metrics());
    if (partitionData != null) {
      builder.withPartition(partitionData);
    }
    return builder.build();
  }
}