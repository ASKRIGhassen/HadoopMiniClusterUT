package com.hadoop.minicluster.common;

import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

public class SparkWrapper {

    private static SparkWrapper instance;
    private static SparkSession spark;


    public static SparkWrapper getInstance() {
        if (instance == null) {
            instance = new SparkWrapper();
        }
        return instance;
    }

    public static SparkSession getSparkSession(String appName) {
        String app = appName != null && !appName.equals("") ? appName : "Default App";
        spark = SparkSession.builder().appName(app).config("spark.master", "local[*]").getOrCreate();
        return spark;
    }

    public static SparkContext getSparkContext(SparkSession sparkSession) {
        if (sparkSession != null) {
            return sparkSession.sparkContext();
        } else if (spark != null) {
            return spark.sparkContext();
        }
        return null;
    }

    public static FileSystem getHadoopFs(SparkContext sparkContext) {
        if (sparkContext != null) {
            try {
                return FileSystem.get(sparkContext.hadoopConfiguration());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    /**
     *  ---------------------------- SparkSession By SparkContext
     * SparkContext context = new SparkContext(new SparkConf().setAppName("spark-ml").setMaster("local[*]")
     *                 .set("spark.hadoop.fs.default.name", "hdfs://localhost:54310").set("spark.hadoop.fs.defaultFS", "hdfs://localhost:54310")
     *                 .set("spark.hadoop.fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName())
     *                 .set("spark.hadoop.fs.hdfs.server", org.apache.hadoop.hdfs.server.namenode.NameNode.class.getName())
     *                 .set("spark.hadoop.conf", org.apache.hadoop.hdfs.HdfsConfiguration.class.getName()));
     *         this.session = SparkSession.builder().sparkContext(context).getOrCreate();
     */

}
