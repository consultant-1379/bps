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
import static com.ericsson.component.aia.services.bps.test.common.TestConstants.H2_DRIVER;
import static com.ericsson.component.aia.services.bps.test.common.TestConstants.JDBC_INPUT_DATASET;
import static com.ericsson.component.aia.services.bps.test.common.TestConstants.USERNAME;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;
import org.junit.Assert;

import com.ericsson.component.aia.services.bps.test.common.TestConstants;

/**
 * MockJdbcTestService helps creating and exposing Jdbc services in embedded mode.
 */
public class MockJdbcTestService extends MockCommonServices {

    /** The Constant LOGGER. */
    private static final Logger LOGGER = Logger.getLogger(MockJdbcTestService.class);

    /** The conn. */
    private Connection connection;

    /** The db location. */
    private String DB_LOCATION;

    /** The jdbc test service. */
    private static volatile MockJdbcTestService jdbcTestService;

    /**
     * Instantiates a new mock jdbc test service.
     *
     * @param testScenarioPath
     *            the test scenario path
     */
    private MockJdbcTestService(final String testScenarioPath) {
        super(testScenarioPath);
    }

    /**
     * Gets the single instance of MockJdbcTestService.
     *
     * @param testScenarioPath
     *            the test scenario path
     * @return single instance of MockJdbcTestService
     */
    public static MockJdbcTestService getInstance(final String testScenarioPath) {
        if (jdbcTestService == null) {
            synchronized (MockJdbcTestService.class) {
                if (jdbcTestService == null) {
                    jdbcTestService = new MockJdbcTestService(testScenarioPath);
                }
            }
        }
        return jdbcTestService;
    }

    /**
     * Gets the connection.
     *
     * @return the connection
     */
    public Connection getConnection() {

        if (connection == null) {

            final String path = tmpDir.toAbsolutePath() + SEPARATOR + "db";
            DB_LOCATION = "jdbc:h2:" + path + SEPARATOR + "H2_JDBC";

            try {
                new File(path).mkdirs();
                Class.forName(H2_DRIVER);
                connection = DriverManager.getConnection(DB_LOCATION, USERNAME, TestConstants.PASSWORD);
                LOGGER.debug("Connection created succesfully");
            } catch (final Exception e) {
                LOGGER.info(e);
                Assert.fail(e.getMessage());
            }
        }
        return connection;
    }

    /**
     * Initialize hive context.
     *
     * @param table
     *            the table
     */
    public void initializeJdbc(final String table) {

        Statement stmt = null;

        try {
            stmt = getConnection().createStatement();
            stmt.executeUpdate("DROP TABLE IF EXISTS " + table);
            stmt.executeUpdate("CREATE TABLE " + table + " AS SELECT * FROM CSVREAD('" + JDBC_INPUT_DATASET + "');");
            LOGGER.debug("Table created");
        } catch (final Exception e) {
            LOGGER.info(e);
            Assert.fail(e.getMessage());
        }
    }

    /**
     * Clean up.
     */
    @Override
    public void cleanUp() {

        if (null != getConnection()) {
            Statement smt1 = null;
            try {
                smt1 = getConnection().createStatement();
                // smt1.executeUpdate("DROP TABLE IF EXISTS JDBC");
                smt1.executeUpdate("SHUTDOWN");
                smt1.close();

            } catch (final Exception e) {
                LOGGER.debug("CleanUp: Exception :", e);
            }

            try {
                getConnection().close();
            } catch (final SQLException e) {
                LOGGER.debug("CleanUp: Exception :", e);
            }
            connection = null;
        }

        if (cleanUp) {
            deleteFolder(tmpDir.toFile());
            super.cleanUp();
        }
    }

    /**
     * Gets the db location.
     *
     * @return the dB_LOCATION
     */
    public String getDB_LOCATION() {
        return DB_LOCATION;
    }
}
