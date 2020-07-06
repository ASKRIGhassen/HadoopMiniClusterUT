package com.tests.common;


import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class HadoopMiniClusterSetup {

    public static Logger LOG = LoggerFactory.getLogger(HadoopMiniClusterSetup.class);


    public static void setHadoopProperties() {


        String hadoopHome = getHadoopHome();
        LOG.info("WINDOWS: Setting hadoop.home.dir: {}", hadoopHome);
        System.setProperty("hadoop.home.dir", hadoopHome);
        System.setProperty("HADOOP_HOME", hadoopHome);
        // Set hadoop.home.dir to point to the windows lib dir
        if (System.getProperty("os.name").startsWith("Windows")) {
            System.load(new File(hadoopHome + Path.SEPARATOR + "bin" + Path.SEPARATOR + "winutils.exe").getAbsolutePath());
            System.load(new File(hadoopHome + Path.SEPARATOR + "lib" + Path.SEPARATOR + "hadoop.dll").getAbsolutePath());
            System.load(new File(hadoopHome + Path.SEPARATOR + "lib" + Path.SEPARATOR + "hdfs.dll").getAbsolutePath());

        }
    }

    public static String getHadoopHome() {

        if (System.getProperty("HADOOP_HOME") != null) {
            LOG.info("HADOOP_HOME: " + System.getProperty("HADOOP_HOME"));
            return System.getProperty("HADOOP_HOME");
        } else if (System.getenv("HADOOP_HOME") != null) { //takes the hadoop home from system environment variable
            LOG.info("HADOOP_HOME: " + System.getenv("HADOOP_HOME"));
            return System.getenv("HADOOP_HOME");
        } else {

            File hadoopHomeDir = new File("." + Path.SEPARATOR + "src/main/resources");
            if (!hadoopHomeDir.exists()) {
                hadoopHomeDir = new File(".." + Path.SEPARATOR + hadoopHomeDir);
                if (!hadoopHomeDir.exists()) {
                    LOG.error("WINDOWS: ERROR: Could not find windows native libs");
                }
            }
            return hadoopHomeDir.getAbsolutePath();
        }
    }
}
