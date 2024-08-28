/*------------------------------------------------------------------------------
 *******************************************************************************
 * COPYRIGHT Ericsson 2017
 *
 * The copyright to the computer program(s) herein is the property of
 * Ericsson Inc. The programs may be used and/or copied only with written
 * permission from Ericsson Inc. or in accordance with the terms and
 * conditions stipulated in the agreement/contract under which the
 * program(s) have been supplied.
 *******************************************************************************
 *----------------------------------------------------------------------------*/
package com.ericsson.component.aia.services.bps.test.common.util;

import static com.ericsson.component.aia.services.bps.test.common.TestConstants.JDBC_INPUT_DATASET;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.postgresql.Driver;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

public class JdbcDataBaseUtility extends CreateTableInDatabase {

    private static final Logger LOGGER = Logger.getLogger(JdbcDataBaseUtility.class);

    /**
     * Create tables in jdbc database and insert the values in table
     *
     * @param tableName
     *            Table to be created
     * @param url
     *            postgres database Url
     * @param user
     *            user name for the database
     * @param password
     *            password for the database
     */

    public static void createTableAndInsertDataInJdbcTable(final String tableName, final String url, final String user, final String password) {

        initializeDatabaseContext(Driver.class.getCanonicalName(), url, user, password);

        createJdbcInputTable(tableName);

        Connection connection = null;
        final PreparedStatement preparedStatement = null;
        FileReader fileReader = null;
        try {

            connection = DriverManager.getConnection(url, user, password);

            final CopyManager cm = new CopyManager((BaseConnection) connection);

            fileReader = new FileReader(JDBC_INPUT_DATASET);
            cm.copyIn("COPY " + tableName + " FROM STDIN WITH DELIMITER ','", fileReader);

        } catch (final SQLException | IOException ex) {
            LOGGER.error(ex.getMessage());
            Assert.fail(ex.getMessage());

        } finally {

            try {
                if (preparedStatement != null) {
                    preparedStatement.close();
                }
                if (connection != null) {
                    connection.close();
                }
                if (fileReader != null) {
                    fileReader.close();
                }

            } catch (SQLException | IOException ex) {
                LOGGER.error(ex.getMessage());
                Assert.fail(ex.getMessage());
            }
        }
    }

    public static void writeDataIntoFileFromJdbcDataBase(final Connection connection, final String tableName, final String outputFile)
            throws Exception {

        final CopyManager copyManager = new CopyManager((BaseConnection) connection);
        final FileWriter fileWriter = new FileWriter(outputFile);
        copyManager.copyOut("COPY " + tableName + " to STDOUT WITH DELIMITER ','", fileWriter);
        fileWriter.close();
    }
}