/*------------------------------------------------------------------------------
 *******************************************************************************
 * COPYRIGHT Ericsson 2016
 *
 * The copyright to the computer program(s) herein is the property of
 * Ericsson Inc. The programs may be used and/or copied only with written
 * permission from Ericsson Inc. or in accordance with the terms and
 * conditions stipulated in the agreement/contract under which the
 * program(s) have been supplied.
 *******************************************************************************
 *----------------------------------------------------------------------------*/
package com.ericsson.component.aia.services.bps.spark.common;

import static com.ericsson.component.aia.services.bps.core.common.Constants.SEPARATOR;

import java.io.File;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

public class MockJavaSparkContext {

    /** The base it folder. */
    public static final String BASE_IT_FOLDER = System.getProperty("java.io.tmpdir") + SEPARATOR + "bps_it";

    /** The root base folder. */
    public static final String ROOT_BASE_FOLDER = BASE_IT_FOLDER + SEPARATOR + "junit_testing_";

    private transient JavaSparkContext sparkContext;
    private transient SQLContext hiveContext;
    private transient Dataset frame;
    private Path tmpDir;
    private static MockJavaSparkContext instance = null;
    /** The Constant USERNAME. */
    private static final String USERNAME = "me";

    /** The Constant PASSWORD. */
    private static final String PASSWORD = "mine";
    /** The derby location. */
    private String DERBY_LOCATION;

    /** The warehouse dir. */
    private String warehouse_dir;

    private MockJavaSparkContext() {
        init();
    }

    // Lazy Initialization (If required then only)
    public static MockJavaSparkContext getInstance() {
        if (instance == null) {
            // Thread Safe. Might be costly operation in some case
            synchronized (MockJavaSparkContext.class) {
                if (instance == null) {
                    instance = new MockJavaSparkContext();
                }
            }
        }
        return instance;
    }

    public void init() {

        final SparkConf sparkConfig = createSparkConf();
        createTestFolder();
        DERBY_LOCATION = "jdbc:derby:" + tmpDir.toAbsolutePath() + SEPARATOR + "metastore_db;";
        warehouse_dir = tmpDir.toAbsolutePath() + SEPARATOR + "hive" + SEPARATOR + "sales_output";
        System.setProperty("javax.jdo.option.ConnectionURL", DERBY_LOCATION + ";create=true;user=" + USERNAME + ";password=" + PASSWORD);
        //Added these configurations so that it don't create folders directly
        System.setProperty("hive.exec.local.scratchdir", tmpDir.toAbsolutePath() + SEPARATOR + "local.scratchdir");
        System.setProperty("hive.exec.scratchdir", tmpDir.toAbsolutePath() + SEPARATOR + "scratchdir");
        System.setProperty("hive.metastore.metadb.dir", tmpDir.toAbsolutePath() + SEPARATOR + "metadbdir");
        System.setProperty("hive.metastore.warehouse.dir", warehouse_dir);
        System.setProperty("hive.querylog.location", tmpDir.toAbsolutePath() + SEPARATOR + "querylog");
        System.setProperty("hive.downloaded.resources.dir", tmpDir.toAbsolutePath() + SEPARATOR + "resources.dir");

        sparkContext = new JavaSparkContext(sparkConfig);

        hiveContext = new HiveContext(sparkContext);
    }

    public void createTestFolder() {

        /*
         * This configuration should be set incase of windows System.setProperty("hadoop.home.dir", "C:\\aia\\components\\hadoop-bin");
         */
        final String dir = ROOT_BASE_FOLDER + RandomStringUtils.randomAlphabetic(8);
        final File fileDir = new File(dir);

        if (fileDir.exists()) {
            fileDir.delete();
        }

        fileDir.mkdir();
        tmpDir = fileDir.toPath();
    }

    private SparkConf createSparkConf() {
        final SparkConf sparkConfig = new SparkConf().setAppName("testing" + new Random().nextInt(1000100)).set("spark.sql.test", "")
                .setMaster("local[32]").set("spark.ui.enabled", "false");

        for (final Map.Entry<String, String> entry : getSparkConfigMap().entrySet()) {

            sparkConfig.set(entry.getKey(), entry.getValue());

        }

        return sparkConfig;
    }

    public Map<String, String> getSparkConfigMap() {

        final String dir = ROOT_BASE_FOLDER + RandomStringUtils.randomAlphabetic(8);
        final File fileDir = new File(dir);

        if (fileDir.exists()) {
            fileDir.delete();
        }

        fileDir.mkdir();
        tmpDir = fileDir.toPath();

        return getSparkConfigMap(tmpDir);
    }

    public Map<String, String> getSparkConfigMap(final Path tmpDir) {

        final String tmpFolder = tmpDir.toAbsolutePath() + SEPARATOR;
        final Map<String, String> sparkConfigMap = new HashMap<>();
        sparkConfigMap.put("spark.local.dir", tmpFolder + "spark_local_dir");
        sparkConfigMap.put("hive.exec.dynamic.partition.mode", "nonstrict");
        sparkConfigMap.put("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sparkConfigMap.put("spark.externalBlockStore.url", tmpFolder + "spark.externalBlockStore.url");
        sparkConfigMap.put("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
        sparkConfigMap.put("hive.metastore.warehouse.dir", tmpFolder + "hive");
        sparkConfigMap.put("spark.externalBlockStore.baseDir", tmpFolder + "spark.externalBlockStore.baseDir");
        sparkConfigMap.put("hive.exec.scratchdir", tmpFolder + "hive.exec.scratchdir");
        sparkConfigMap.put("hive.downloaded.resources.dir", tmpDir + SEPARATOR + "hive.resources.dir_" + System.currentTimeMillis());
        sparkConfigMap.put("hive.querylog.location", tmpFolder + "querylog.location");
        sparkConfigMap.put("hive.exec.local.scratchdir", tmpFolder + "hive.exec.local.scratchdir");
        sparkConfigMap.put("hive.downloaded.resources.dir", tmpFolder + "hive.downloaded.resources.dir");
        sparkConfigMap.put("hive.metadata.export.location", tmpFolder + "hive.metadata.export.location");
        sparkConfigMap.put("hive.metastore.metadb.dir", tmpFolder + "hive.metastore.metadb.dir");
        sparkConfigMap.put("hive.merge.sparkfiles", "true");

        return sparkConfigMap;
    }

    public void tearDown() {
        hiveContext = null;
        sparkContext.stop();
        sparkContext = null;
        synchronized (MockJavaSparkContext.class) {
            instance = null;
        }
    }

    /**
     * @return the sparkContext
     */
    public JavaSparkContext getSparkContext() {
        return sparkContext;
    }

    /**
     * @param sparkContext
     *            the sparkContext to set
     */
    public void setSparkContext(final JavaSparkContext sparkContext) {
        this.sparkContext = sparkContext;
    }

    /**
     * @return the frame
     */
    public Dataset getFrame() {
        return frame;
    }

    /**
     * @param frame
     *            the frame to set
     */
    public void setFrame(final Dataset frame) {
        this.frame = frame;
    }
}