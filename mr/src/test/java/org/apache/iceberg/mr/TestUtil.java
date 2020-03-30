package org.apache.iceberg.mr;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class TestUtil {
  /**
   * Provides setup/teardown of a MiniDFSCluster for tests that need one.
   */
  public static class MiniDFSTest {
    private static Configuration conf = null;
    private static MiniDFSCluster cluster = null;
    private static FileSystem dfs = null;
    private static FileSystem lfs = null;

    protected static Configuration getConfiguration() {
      return conf;
    }

    protected static FileSystem getDFS() {
      return dfs;
    }

    protected static FileSystem getFS() {
      return lfs;
    }

    @BeforeClass
    //@SuppressWarnings("deprecation")
    public static void setupFS() throws IOException {
      if (cluster == null) {
        Configuration c = new Configuration();
        // if this fails with "The directory is already locked" set umask to 0022
        cluster = new MiniDFSCluster(c, 1, true, null);

        dfs = cluster.getFileSystem();
        conf = new Configuration(dfs.getConf());
        lfs = FileSystem.getLocal(conf);
      }
    }

    @AfterClass
    public static void teardownFS() throws IOException {
      dfs = null;
      lfs = null;
      conf = null;
      if (cluster != null) {
        cluster.shutdown();
        cluster = null;
      }
    }
  }
}