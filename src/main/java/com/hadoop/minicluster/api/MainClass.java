package com.hadoop.minicluster.api;

import com.github.sakserv.minicluster.impl.HdfsLocalCluster;
import com.hadoop.minicluster.common.PropertiesUtils;
import com.hadoop.minicluster.common.SparkWrapper;
import com.hadoop.minicluster.common.WindowsLibUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class MainClass {
    private static HdfsLocalCluster hdfsLocalCluster;

    static {
        WindowsLibUtils.setHadoopHome();
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
    }

    public static void main(String[] args) {
        try {
            hdfsLocalCluster.start();

           SparkSession spark = SparkSession.builder().appName("App Test").config("spark.master", "local[*]").getOrCreate();

            FileSystem fs = SparkWrapper.getHadoopFs(spark.sparkContext());
            fs.copyFromLocalFile(new Path("D:/Spark-The-Definitive-Guide/data/flight-data/json/2010-summary.json"), new Path("/flight-data/2010-summary.json"));
            Dataset df = spark.read().option("inferSchema", "true").json("/flight-data/2010-summary.json");
            df.printSchema();
            df.show();

            hdfsLocalCluster.stop();

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
