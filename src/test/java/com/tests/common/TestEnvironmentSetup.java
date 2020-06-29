package com.tests.common;

import com.github.sakserv.minicluster.impl.HdfsLocalCluster;
import com.hadoop.minicluster.common.PropertiesUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class TestEnvironmentSetup extends HadoopMiniClusterSetup {

    private static HdfsLocalCluster hdfsLocalCluster;
    private static SparkSession spark;

    @BeforeClass
    public static void setup() throws Exception {
        setHadoopHome();
        Properties properties = PropertiesUtils.readPropertiesFile("default.properties");
        hdfsLocalCluster = new HdfsLocalCluster.Builder()
                .setHdfsNamenodePort(Integer.parseInt(properties.getProperty(PropertiesUtils.HDFS_NAMENODE_PORT)))
                .setHdfsTempDir(properties.getProperty(PropertiesUtils.HDFS_TEMP_DIR))
                .setHdfsNumDatanodes(Integer.parseInt(properties.getProperty(PropertiesUtils.HDFS_NUM_DATANODE)))
                .setHdfsEnablePermissions(
                        Boolean.parseBoolean(properties.getProperty(PropertiesUtils.HDFS_ENABLE_PERMISSIONS)))
                .setHdfsFormat(Boolean.parseBoolean(properties.getProperty(PropertiesUtils.HDFS_FORMAT)))
                .setHdfsEnableRunningUserAsProxyUser(Boolean.parseBoolean(
                        properties.getProperty(PropertiesUtils.HDFS_ENABLE_RUNNING_USER)))
                .setHdfsConfig(new Configuration())
                .build();
        hdfsLocalCluster.start();
        spark = SparkSession.builder().appName("TestEnv").master("local[*]").getOrCreate();

    }

    @Test
    public void testWithDateColumnExample() {
        StructField[] columns = new StructField[]{
                new StructField("first_name", DataTypes.StringType, false, Metadata.empty()),
                new StructField("last_name", DataTypes.StringType, false, Metadata.empty()),
                new StructField("age", DataTypes.IntegerType, false, Metadata.empty())
        };

        StructType schema = new StructType(columns);
        List<Row> data = new ArrayList<Row>();
        data.add(RowFactory.create("Layene","ASKRI",1));
        data.add(RowFactory.create("Ghassen","ASKRI",30));

        Dataset<Row> df = spark.createDataFrame(data,schema);
        df.show();
        assert(df.count()== 2);
    }

    @After
    public void clearAndStop() throws Exception {
        hdfsLocalCluster.stop(true);
    }
}
