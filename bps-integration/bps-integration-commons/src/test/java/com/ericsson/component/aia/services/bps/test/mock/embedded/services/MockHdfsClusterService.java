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
package com.ericsson.component.aia.services.bps.test.mock.embedded.services;

import static com.ericsson.component.aia.services.bps.core.common.Constants.SEPARATOR;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.log4j.Logger;
import org.junit.Assert;

import com.ericsson.component.aia.services.bps.test.services.TestHdfsContext;

/**
 * MockHdfsClusterService helps creating and exposing Hdfs services in embedded mode.
 */
public class MockHdfsClusterService extends MockCommonServices {

    /** The Constant LOGGER. */
    private static final Logger LOGGER = Logger.getLogger(MockHdfsClusterService.class);

    /** The hdfs cluster. */
    private static volatile MiniDFSCluster hdfsCluster;

    /** The conf. */
    private Configuration conf;

    /** The mock hdfs cluster. */
    private static volatile MockHdfsClusterService mockHdfsCluster = null;

    /** The file sys. */
    private FileSystem fileSys;

    /**
     * Instantiates a new mock hdfs cluster service.
     *
     * @param testScenarioPath
     *            the test scenario path
     */
    private MockHdfsClusterService(final String testScenarioPath) {
        super(testScenarioPath);
        initializeHdfsCluster();
    }

    /**
     * Gets the single instance of MockHdfsClusterService.
     *
     * @param testScenarioPath
     *            the test scenario path
     * @return single instance of MockHdfsClusterService
     */
    public static MockHdfsClusterService getInstance(final String testScenarioPath) {
        if (mockHdfsCluster == null) {
            synchronized (MockHdfsClusterService.class) {
                if (mockHdfsCluster == null) {
                    mockHdfsCluster = new MockHdfsClusterService(testScenarioPath + SEPARATOR + MockHdfsClusterService.class.getSimpleName());
                }
            }
        }
        return mockHdfsCluster;
    }

    /**
     * Clean up.
     */
    @Override
    public void cleanUp() {
        if (hdfsCluster != null) {
            hdfsCluster.shutdown();
            hdfsCluster = null;
        }
        if (cleanUp) {
            cleanUp();
        }
    }

    /**
     * Initialize hdfs cluster.
     */
    private void initializeHdfsCluster() {
        if (hdfsCluster == null) {
            synchronized (TestHdfsContext.class) {
                final File testDataPath = tmpDir.toFile();
                conf = new HdfsConfiguration();
                final File testDataCluster1 = new File(testDataPath, "cluster1");
                final String c1PathStr = testDataCluster1.getAbsolutePath();
                conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, c1PathStr);

                // Source file in the local file system
                try {
                    hdfsCluster = new MiniDFSCluster.Builder(conf).build();
                    fileSys = hdfsCluster.getFileSystem();
                    // dfsCluster = new MiniDFSCluster(conf, 1, true, null);
                    assertNotNull("Cluster has a file system", hdfsCluster.getFileSystem());
                } catch (final IOException e) {
                    LOGGER.info(e);
                    Assert.fail(e.getMessage());
                }
            }
        }
    }

    /**
     * Gets the file sys.
     *
     * @return the fileSys
     */
    public FileSystem getFileSys() {
        return fileSys;
    }

    /**
     * Gets the hdfs cluster.
     *
     * @return the hdfsCluster
     */
    public MiniDFSCluster getHdfsCluster() {
        return hdfsCluster;
    }

    /**
     * Gets the conf.
     *
     * @return the conf
     */
    public Configuration getConf() {
        return conf;
    }
}