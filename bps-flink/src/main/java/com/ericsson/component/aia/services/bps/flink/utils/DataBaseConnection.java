package com.ericsson.component.aia.services.bps.flink.utils;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.services.bps.core.exception.BpsRuntimeException;

/**
 * Helps to create a new instance of data base connection and reset the connection.
 */
public class DataBaseConnection {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataBaseConnection.class);

    private static volatile DataBaseConnection dataBaseConnection;

    private final String drivername;

    private final String username;

    private final String password;

    private final String dbURL;

    private BasicDataSource connectionPool;

    private DataBaseConnection(final String drivername, final String username, final String password, final String dbURL) {
        this.drivername = drivername;
        this.username = username;
        this.password = password;
        this.dbURL = dbURL;

    }

    /**
     * Creates a singleton instance of the data abse connection in order to facilitate Db connection
     *
     * @param drivername
     *            jdbc driver name
     * @param username
     *            user name
     * @param password
     *            password for the data base
     * @param dbURL
     *            data base connection url
     * @return returns the data base connection
     */
    public static DataBaseConnection getInstance(final String drivername, final String username, final String password, final String dbURL) {
        if (dataBaseConnection == null) {
            synchronized (DataBaseConnection.class) {
                dataBaseConnection = new DataBaseConnection(drivername, username, password, dbURL);
            }
        }
        return dataBaseConnection;

    }

    /**
     * Create connection with jdbc database based on provided uri in flow.xml.
     *
     * @return - Connection object from the database
     * @throws ClassNotFoundException
     *             throws classNotFoundException
     * @throws SQLException
     *             throws SqlException
     * @throws InstantiationException
     *             throws InstantiationException
     * @throws IllegalAccessException
     *             throws IllegalAccessException
     */
    public Connection createDataBaseConnection() {
        Connection connection;
        try {
            connectionPool = new BasicDataSource();
            connectionPool.setUsername(username);
            connectionPool.setPassword(password);
            connectionPool.setDriverClassName(drivername);
            connectionPool.setUrl(dbURL);
            connection = connectionPool.getConnection();
            LOGGER.trace("=======Jdbc Connection created=======");

        } catch (final SQLException e) {
            LOGGER.error("Exception occurred while creating  Database Connection :" + e.getMessage());
            throw new BpsRuntimeException(e);
        }
        return connection;

    }

    /**
     * Reset the connection and singleton instance in order to create new instance for another test.
     */
    public void cleanUpDatabaseConnectionInstance() {
        try {
            dataBaseConnection = null;
            LOGGER.trace("Database Connection Closed");
        } catch (Exception e) {
            throw new BpsRuntimeException(e.getMessage());
        }
    }
}
