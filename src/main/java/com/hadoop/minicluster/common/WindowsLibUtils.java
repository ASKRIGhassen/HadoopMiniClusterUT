package com.hadoop.minicluster.common;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class WindowsLibUtils {

    public static Logger LOG = LoggerFactory.getLogger(WindowsLibUtils.class);

    public static void setHadoopHome() {

        // Set hadoop.home.dir to point to the windows lib dir
        if (System.getProperty("os.name").startsWith("Windows")) {

            String windowsLibDir = getHadoopHome();

            LOG.info("WINDOWS: Setting hadoop.home.dir: {}", windowsLibDir);
            System.setProperty("hadoop.home.dir", windowsLibDir);
            System.setProperty("HADOOP_HOME", windowsLibDir);
            System.load(new File(windowsLibDir + Path.SEPARATOR + "bin" + Path.SEPARATOR + "winutils.exe").getAbsolutePath());
            System.load(new File(windowsLibDir + Path.SEPARATOR + "lib" + Path.SEPARATOR + "hadoop.dll").getAbsolutePath());
            System.load(new File(windowsLibDir + Path.SEPARATOR + "lib" + Path.SEPARATOR + "hdfs.dll").getAbsolutePath());
            //System.load(new File(windowsLibDir + Path.SEPARATOR + "lib" + Path.SEPARATOR + "libwinutils.lib").getAbsolutePath());


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

            File windowsLibDir = new File("." + Path.SEPARATOR + "src/main/resources");
            if (!windowsLibDir.exists()) {
                windowsLibDir = new File(".." + Path.SEPARATOR + windowsLibDir);
                if (!windowsLibDir.exists()) {
                    LOG.error("WINDOWS: ERROR: Could not find windows native libs");
                }
            }
            return windowsLibDir.getAbsolutePath();
        }
    }

}
