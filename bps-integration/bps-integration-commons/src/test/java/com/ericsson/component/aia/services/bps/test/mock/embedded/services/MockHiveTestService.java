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
import static com.ericsson.component.aia.services.bps.test.common.TestConstants.DATA_FILE_NAME;
import static com.ericsson.component.aia.services.bps.test.common.TestConstants.HIVE_DRIVER;
import static com.ericsson.component.aia.services.bps.test.common.TestConstants.USERNAME;
import static com.ericsson.component.aia.services.bps.test.common.TestConstants.WORKING_DIR;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import com.ericsson.component.aia.services.bps.core.common.DataFormat;
import com.ericsson.component.aia.services.bps.test.common.TestConstants;
import com.ericsson.component.aia.services.bps.test.common.util.CreateTableInDatabase;

/**
 * MockHiveTestService helps creating and exposing Hive services in embedded mode.
 */
public class MockHiveTestService extends MockCommonServices {

    /** The conn. */
    private Connection conn;

    /** The Constant LOGGER. */
    private static final Logger LOGGER = Logger.getLogger(MockHiveTestService.class);

    /** The derby location. */
    private String DERBY_LOCATION;

    /** The warehouse dir. */
    private String warehouse_dir;

    /** The hive test service. */
    private static volatile MockHiveTestService hiveTestService;

    /** The input table folder. */
    private String INPUT_TABLE_FOLDER;

    /**
     * Instantiates a new mock hive test service.
     *
     * @param testScenarioPath
     *            the test scenario path
     */
    private MockHiveTestService(final String testScenarioPath) {
        super(testScenarioPath);
    }

    /**
     * Gets the single instance of MockHiveTestService.
     *
     * @param testScenarioPath
     *            the test scenario path
     * @return single instance of MockHiveTestService
     */
    public static MockHiveTestService getInstance(final String testScenarioPath) {
        if (hiveTestService == null) {
            synchronized (MockHiveTestService.class) {
                if (hiveTestService == null) {
                    hiveTestService = new MockHiveTestService(testScenarioPath + SEPARATOR + MockHiveTestService.class.getSimpleName());
                    hiveTestService.initialForEmbeddedMode();
                }
            }
        }
        return hiveTestService;
    }

    /**
     * Creates the hive input table.
     *
     * @param inputDataFormat
     *            the input data format
     * @param tableIpName
     *            the table ip name
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    public void createHiveInputTable(final String inputDataFormat, final String tableIpName) throws IOException {
        createInputDataFolder(inputDataFormat);
        CreateTableInDatabase.initializeDatabaseContext(HIVE_DRIVER, "jdbc:hive2://", "", "");
        CreateTableInDatabase.createHiveInputTable(inputDataFormat, tableIpName, INPUT_TABLE_FOLDER);
        shutdown();
    }

    /**
     * Shutdown.
     */
    private void shutdown() {

        try {
            Class.forName(HIVE_DRIVER);
            conn = DriverManager.getConnection("jdbc:hive2://", "", "");
        } catch (final ClassNotFoundException e) {
            e.printStackTrace();
        } catch (final SQLException e) {
            e.printStackTrace();
        }

        if (conn != null) {

            try {
                DriverManager.getConnection(DERBY_LOCATION + ";shutdown=true");
            } catch (final SQLException ex) {
                if (((ex.getErrorCode() == 50000) && ("XJ015".equals(ex.getSQLState())))) {
                    LOGGER.debug("Derby shut down  normally");
                } else {
                    LOGGER.debug("Derby did not shut down  normally");
                    LOGGER.debug("Derby did not shut down  normally", ex);
                }
            }

            try {
                conn.close();
            } catch (final SQLException e) {
                LOGGER.debug("shutdown, SQLException", e);
            }
        }
    }

    /**
     * Initial for embedded mode.
     */
    private void initialForEmbeddedMode() {
        DERBY_LOCATION = "jdbc:derby:" + tmpDir.toAbsolutePath() + SEPARATOR + "metastore_db;";
        warehouse_dir = tmpDir.toAbsolutePath() + SEPARATOR + "hive" + SEPARATOR + "sales_output";

        System.setProperty("javax.jdo.option.ConnectionURL",
                DERBY_LOCATION + "create=true;user=" + USERNAME + ";password=" + TestConstants.PASSWORD);
        //Added these configurations so that it don't create folders directly
        System.setProperty("hive.exec.local.scratchdir", tmpDir.toAbsolutePath() + SEPARATOR + "local.scratchdir");
        System.setProperty("hive.exec.scratchdir", tmpDir.toAbsolutePath() + SEPARATOR + "scratchdir");
        System.setProperty("hive.metastore.metadb.dir", tmpDir.toAbsolutePath() + SEPARATOR + "metadbdir");
        System.setProperty("hive.metastore.warehouse.dir", warehouse_dir);
        System.setProperty("hive.querylog.location", tmpDir.toAbsolutePath() + SEPARATOR + "querylog");
        System.setProperty("hive.downloaded.resources.dir", tmpDir.toAbsolutePath() + SEPARATOR + "resources.dir");
    }

    /**
     * Clean up.
     */
    @Override
    public void cleanUp() {

        if (null != conn) {
            try {
                conn.close();
            } catch (final SQLException e) {
                LOGGER.info("cleanUp, SQLException", e);
            }
        }

        if (cleanUp) {
            deleteFolder(new File(INPUT_TABLE_FOLDER));
            super.cleanUp();
        }
    }

    /**
     * Creates the input data folder.
     *
     * @param inputDataFormat
     *            the input data format
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    public void createInputDataFolder(final String inputDataFormat) throws IOException {
        INPUT_TABLE_FOLDER = tmpDir + SEPARATOR + "Hive_InputData_Folder" + System.currentTimeMillis();
        final String url = WORKING_DIR + "/data/input_data_set/hive/" + DATA_FILE_NAME + getExtension(inputDataFormat);
        final File srcFile = new File(url);
        FileUtils.forceMkdir(new File(INPUT_TABLE_FOLDER));
        FileUtils.copyFileToDirectory(srcFile, new File(INPUT_TABLE_FOLDER));
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
     * Gets the warehouse dir.
     *
     * @return the warehouse_dir
     */
    public String getWarehouse_dir() {
        return warehouse_dir;
    }
}
