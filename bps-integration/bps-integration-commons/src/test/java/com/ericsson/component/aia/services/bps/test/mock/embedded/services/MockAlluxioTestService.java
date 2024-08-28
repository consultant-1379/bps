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

import static com.ericsson.component.aia.services.bps.test.common.TestConstants.DATA_FILE_NAME;
import static com.ericsson.component.aia.services.bps.test.common.TestConstants.INPUT_DATA_FOLDER;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.junit.Assert;

import com.ericsson.component.aia.services.bps.core.common.DataFormat;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.master.LocalAlluxioCluster;

/**
 * MockAlluxioTestService helps creating and exposing Alluxio services in embedded mode.
 */
public class MockAlluxioTestService extends MockCommonServices {

    /** The Constant LOGGER. */
    private static final Logger LOGGER = Logger.getLogger(MockAlluxioTestService.class);

    /** The mock alluxio cluster. */
    private static volatile MockAlluxioTestService mockAlluxioCluster = null;

    /** The local alluxio cluster. */
    private static volatile LocalAlluxioCluster localAlluxioCluster;

    /** The test scenario path. */
    private String testScenarioPath;

    /** The fs. */
    private FileSystem fs;

    /**
     * Instantiates a new mock alluxio test service.
     *
     * @param testScenarioPath
     *            the test scenario path
     * @throws Exception
     *             the exception
     */
    private MockAlluxioTestService(final String testScenarioPath) throws Exception {
        super(testScenarioPath);
        this.testScenarioPath = testScenarioPath;
        initializeCluster();
        start();
        fs = FileSystem.Factory.get();
    }

    /**
     * Gets the single instance of MockAlluxioTestService.
     *
     * @param testScenarioPath
     *            the test scenario path
     * @return single instance of MockAlluxioTestService
     * @throws Exception
     *             the exception
     */
    public static MockAlluxioTestService getInstance(final String testScenarioPath) throws Exception {
        if (mockAlluxioCluster == null) {
            synchronized (MockAlluxioTestService.class) {
                if (mockAlluxioCluster == null) {
                    mockAlluxioCluster = new MockAlluxioTestService(testScenarioPath);
                }
            }
        }
        return mockAlluxioCluster;
    }

    /**
     * Clean up.
     */
    @Override
    public void cleanUp() {
        if (localAlluxioCluster != null) {

            try {
                localAlluxioCluster.stop();
                localAlluxioCluster.stopFS();
            } catch (final Exception e) {
                LOGGER.debug(e);
            }
        }
        super.cleanUp();
    }

    /**
     * Copy files.
     *
     * @param inputDataFormat
     *            the input data format
     */
    public void copyFiles(final String inputDataFormat) {
        final String INPUT_FILE = INPUT_DATA_FOLDER + DATA_FILE_NAME + getExtension(inputDataFormat);
        final FileOutStream os;
        try {
            os = fs.createFile(new AlluxioURI("/" + DATA_FILE_NAME + getExtension(inputDataFormat)));
            os.write(FileUtils.readFileToByteArray(new File(INPUT_FILE)));
            os.close();
        } catch (final Exception e) {
            LOGGER.info(e);
            Assert.fail(e.getMessage());
        }
    }

    /**
     * Mk dirs ret path.
     *
     * @param path
     *            the path
     * @return the string
     */
    private String mkDirsRetPath(final String path) {
        new File(path).mkdirs();
        return path;
    }

    /**
     * Start.
     *
     * @throws Exception
     *             the exception
     */
    public void start() throws Exception {
        Configuration.set(PropertyKey.TEST_MODE, "true");
        Configuration.set(PropertyKey.HOME, mkDirsRetPath(testScenarioPath + "/Alluxio/HOME"));
        Configuration.set(PropertyKey.WORK_DIR, mkDirsRetPath(testScenarioPath + "/Alluxio/WORK_DIR"));
        Configuration.set(PropertyKey.MASTER_JOURNAL_FOLDER, mkDirsRetPath(testScenarioPath + "/Alluxio/journal"));
        Configuration.set(PropertyKey.MASTER_JOURNAL_FOLDER, mkDirsRetPath(testScenarioPath + "/Alluxio/journalFolder"));
        Configuration.set(PropertyKey.UNDERFS_ADDRESS, mkDirsRetPath(testScenarioPath + "/Alluxio/ufsAddress"));
        Configuration.set(PropertyKey.WORKER_DATA_FOLDER, mkDirsRetPath(testScenarioPath + "/Alluxio/datastore"));
        localAlluxioCluster = new LocalAlluxioCluster();
        localAlluxioCluster.initConfiguration();
        localAlluxioCluster.start();
    }

    /**
     * Gets the local alluxio cluster.
     *
     * @return the localAlluxioCluster
     */
    public LocalAlluxioCluster getLocalAlluxioCluster() {
        return localAlluxioCluster;
    }

    /**
     * Gets the extension.
     *
     * @param dataFormat
     *            the data format
     * @return the extension
     */
    protected String getExtension(final String dataFormat) {
        return "." + DataFormat.valueOf(dataFormat).dataFormat;
    }

    /**
     * Initialize cluster.
     */
    private void initializeCluster() {
        localAlluxioCluster = new LocalAlluxioCluster();
    }

    /**
     * Gets the file system.
     *
     * @return the fs
     */
    public FileSystem getFileSystem() {
        return fs;
    }
}
