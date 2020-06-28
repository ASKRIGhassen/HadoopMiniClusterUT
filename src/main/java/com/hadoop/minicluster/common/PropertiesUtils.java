package com.hadoop.minicluster.common;

import java.io.InputStream;
import java.util.Properties;

public class PropertiesUtils {

    public static final String HDFS_NAMENODE_PORT ="hdfs.namenode.port";
    public static final String HDFS_NAMENODE_HTTP_PORT="hdfs.namenode.http.port";
    public static final String HDFS_TEMP_DIR ="hdfs.temp.dir";
    public static final String HDFS_NUM_DATANODE ="hdfs.num.datanodes";
    public static final String HDFS_ENABLE_PERMISSIONS="hdfs.enable.permissions";
    public static final String HDFS_FORMAT="hdfs.format";
    public static final String HDFS_ENABLE_RUNNING_USER="hdfs.enable.running.user.as.proxy.user";

    public static Properties readPropertiesFile(String fileName) {

        Properties properties = new Properties();
        InputStream inputStream = PropertiesUtils.class.getClassLoader().getResourceAsStream(fileName);
        try {
            if (inputStream != null) {
                properties.load(inputStream);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return properties;
    }
}
