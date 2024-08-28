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

import static com.ericsson.component.aia.services.bps.test.common.TestConstants.DEFAULT_INPUT_DATASET;

import java.io.FileReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.log4j.Logger;
import org.junit.Assert;

/**
 * Helps to create table in Hive and Jdbc Database.
 *
 */
public abstract class CreateTableInDatabase {

    private static final Logger LOGGER = Logger.getLogger(CreateTableInDatabase.class);

    private static Connection conn;

    public static void createHiveInputTable(final String inputDataFormat, final String tableIpName, final String inputTableFolder) {
        Statement stmt = null;

        final String columns = getTableColumns();

        try {
            stmt = conn.createStatement();
            stmt.executeUpdate("create external table IF NOT EXISTS " + tableIpName + " ( " + columns
                    + " ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS " + inputDataFormat + " location '" + inputTableFolder
                    + "' tblproperties ('skip.header.line.count'='1')");
            LOGGER.debug("External table created");
        } catch (final Exception e) {
            LOGGER.info("Unable to create external table, Exception", e);
            Assert.fail(e.getMessage());
        }
    }

    public static void createJdbcInputTable(final String tableName) {

        PreparedStatement sqlStatement = null;

        final String columns = getTableColumns();

        try {

            final String query = "CREATE TABLE IF NOT EXISTS " + tableName + "(" + columns + ")";
            sqlStatement = conn.prepareStatement(query);
            sqlStatement.execute();
            LOGGER.debug("External table created");
        } catch (final Exception e) {
            LOGGER.info("Unable to create external table, Exception", e);
            Assert.fail(e.getMessage());
        }

    }

    private static String getTableColumns() {
        String columns = "";

        final Reader in;
        Iterable<CSVRecord> records = null;

        try {
            in = new FileReader(DEFAULT_INPUT_DATASET);
            records = CSVFormat.RFC4180.parse(in);
        } catch (final Exception e) {
            LOGGER.info("Unable to read INPUT_FILE, Exception", e);
            Assert.fail(e.getMessage());
        }

        for (final CSVRecord record : records) {

            final int size = record.size();

            for (int i = 0; i < size; ++i) {
                if (i == 0) {
                    columns = record.get(i) + " varchar(500) ";
                } else {
                    columns = columns + ", " + record.get(i) + " varchar(500) ";
                }
            }
            break;
        }
        return columns;
    }

    /**
     * Initialize hive context.
     * 
     * @param canonicalName
     * @param databaseDriverUrl
     */
    public static void initializeDatabaseContext(final String canonicalName, final String databaseDriverUrl, final String userName,
                                                 final String password) {

        try {
            Class.forName(canonicalName);
            conn = DriverManager.getConnection(databaseDriverUrl, userName, password);
        } catch (final ClassNotFoundException e) {
            Assert.fail(e.getMessage());
        } catch (final SQLException e) {
            Assert.fail(e.getMessage());
        }
    }
}