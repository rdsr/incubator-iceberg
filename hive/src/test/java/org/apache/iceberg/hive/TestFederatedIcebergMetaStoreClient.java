package org.apache.iceberg.hive;

import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.StandaloneHiveRunner;
import com.klarna.hiverunner.annotations.HiveSQL;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.HashMap;

@RunWith(StandaloneHiveRunner.class)
public class TestFederatedIcebergMetaStoreClient {
  @HiveSQL(files = {}, autoStart = false)
  private HiveShell shell;

  private static TestHiveMetastore metastore;
  private static HiveCatalog catalog;
  private static HiveMetaStoreClient metastoreClient;

  @BeforeClass
  public static void setup() throws Exception {
    metastore = new TestHiveMetastore();
    metastore.start();

    HiveConf conf = metastore.hiveConf();
    // create test db
    metastoreClient = new HiveMetaStoreClient(conf);
    String dbPath = metastore.getDatabasePath("db");
    Database db = new Database("db", "description", dbPath, new HashMap<>());
    metastoreClient.createDatabase(db);
    // setup Iceberg Catalog
    catalog = HiveCatalogs.loadCatalog(conf);
  }

  @Test
  public void test() {
    // setup HiveRunner
    shell.setHiveConfValue(
            HiveConf.ConfVars.METASTOREURIS.varname,
            metastore.hiveConf().getVar(HiveConf.ConfVars.METASTOREURIS));
    shell.setHiveConfValue("metastore.client.impl", FederatedIcebergMetaStoreClient.class.getName());
    shell.start();

    Schema schema = new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.optional(1, "data", Types.StringType.get())
    );
    catalog.createTable(TableIdentifier.of("db", "t"), schema);

    shell.executeQuery("describe formatted db.t").forEach(System.out::println);
  }
}

