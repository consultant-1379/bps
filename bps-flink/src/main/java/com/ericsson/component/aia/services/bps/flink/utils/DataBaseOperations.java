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
package com.ericsson.component.aia.services.bps.flink.utils;

import static com.ericsson.component.aia.services.bps.flink.utils.SaveMode.Append;
import static com.ericsson.component.aia.services.bps.flink.utils.SaveMode.Overwrite;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.services.bps.core.exception.BpsRuntimeException;

/**
 * DataBaseOperations class to help setting database connection and to execute query on database.
 */
public class DataBaseOperations {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataBaseOperations.class);
    private static final Map<String, String> schema = new LinkedHashMap<String, String>();
    private final List<String> columnNames = new LinkedList<String>();
    private final List<String> columnTypes = new LinkedList<String>();
    private Connection connection;
    private ResultSet resultSet;
    private String username;
    private String password;
    private String drivername;
    private String dbURL;
    private String tableName;
    private String query;
    private Map<String, String> schemaMap;
    private DataBaseConnection dataBaseConnection;

    /**
     * Takes output schema pojo, iterates over the fields declared in the pojo and returns the map of columns and types
     *
     * @param outputSchema
     *            pojo schema class
     * @return Map of columns and type for the table
     */

    public static Map<String, String> convertsToDataBaseSchemaMap(final String outputSchema) {
        try {
            final Class pojo = Class.forName(outputSchema);
            final Object obj = pojo.newInstance();
            final java.lang.reflect.Field[] typeFields = obj.getClass().getDeclaredFields();
            final Map<String, String> typeFieldNames = new LinkedHashMap<String, String>();
            for (int i = 0; i < typeFields.length; i++) {
                final boolean isAccessible = typeFields[i].isAccessible();
                if (!isAccessible) {
                    typeFields[i].setAccessible(true);
                    typeFieldNames.put(typeFields[i].getName(), JavaToJdbcTypeConverter.getTypeInfo(typeFields[i].getType()));
                }
                typeFields[i].setAccessible(isAccessible);
            }
            return typeFieldNames;
        } catch (final ClassNotFoundException | IllegalAccessException | InstantiationException e) {
            LOGGER.error("Exception occurred " + e.getMessage());
            throw new BpsRuntimeException(e);
        }
    }

    /**
     * Retrieves the Sql Data types for writing data into jdbc database
     *
     * @param dataTypeInfo
     *            Data Types to be converted in sql data types
     * @return returns sql Data types
     */
    public static int[] getSqlInfoTypeArray(final Map<String, String> dataTypeInfo) {
        final List<Integer> sqlDataType = new LinkedList<>();
        int index = 0;
        final int[] typeArray = new int[dataTypeInfo.size()];
        for (final Map.Entry<String, String> entry : dataTypeInfo.entrySet()) {
            sqlDataType.add(JavaToJdbcTypeConverter.sqlTypes(entry.getValue()));
            typeArray[index] = JavaToJdbcTypeConverter.sqlTypes(entry.getValue());
            ++index;

        }
        return typeArray;
    }

    /**
     * A builder used to set parameters to the output format's configuration in a fluent way.
     *
     * @return builder
     */
    public static DataBaseOperationsPropertyBuilder buildPropertyBuilder() {
        return new DataBaseOperationsPropertyBuilder();
    }

    /**
     * To check whether table exists in the jdbc database or if it does then it will extract the data types from database and converts those data
     * types in flink data types and return RaTypeInfo.
     *
     * @return TypeInformation[] An array of TypeInformation of columns supported by flink
     * @throws SQLException
     *             throws sqlException and finally throws BpsRunTimeException to stop flink job runner
     */
    public RowTypeInfo getRawTypeInfo() {

        final List<TypeInformation<?>> flinkTypesList = new LinkedList<TypeInformation<?>>();
        try {
            prepareDataBaseConnection();
            final TypeInformation<?>[] filedTypes;
            final RowTypeInfo rowTypeInfo;
            final ResultSetMetaData resultSetMetaData = getResultSetMetaData();
            extractSchemaInfomation(flinkTypesList, resultSetMetaData);
            final int arrayListSize = flinkTypesList.size();
            filedTypes = flinkTypesList.toArray(new TypeInformation[arrayListSize]);
            rowTypeInfo = new RowTypeInfo(filedTypes);
            return rowTypeInfo;
        } catch (final SQLException | ClassNotFoundException e) {
            LOGGER.error("Exception occurred " + e);
            throw new BpsRuntimeException(e);
        } finally {
            closeResultSet(resultSet);
            closeConnection();
        }
    }

    private void extractSchemaInfomation(final List<TypeInformation<?>> flinkTypesList, final ResultSetMetaData resultSetMetaData)
            throws SQLException, ClassNotFoundException {
        final int numberCols = resultSetMetaData.getColumnCount();
        for (int i = 1; i <= numberCols; i++) {
            final Class<?> className = Class.forName(resultSetMetaData.getColumnClassName(i));
            columnNames.add(resultSetMetaData.getColumnLabel(i));
            columnTypes.add(resultSetMetaData.getColumnTypeName(i));
            schema.put(resultSetMetaData.getColumnLabel(i), resultSetMetaData.getColumnTypeName(i));
            if (BasicTypeInfo.getInfoFor(className) != null) {
                flinkTypesList.add(BasicTypeInfo.getInfoFor(className));
            } else if (SqlTimeTypeInfo.getInfoFor(className) != null) {
                flinkTypesList.add(SqlTimeTypeInfo.getInfoFor(className));
            }
        }
    }

    public Map<String, String> getSchema() {
        return schema;
    }

    public void setConnection(final Connection connection) {
        this.connection = connection;
    }

    protected void setTableName(final String tableName) {
        this.tableName = tableName;
    }

    /**
     * Perform database operations based on the mode type specified in flow file.
     *
     * @param saveMode
     *            save mode given in flow xml file
     */
    public void tableOperations(final String saveMode) {
        try {
            prepareDataBaseConnection();
            switch (getMode(saveMode)) {
                case Append:
                    createNewTableIfDoesNotExistInDatabase();
                    break;
                case Overwrite:
                    dropAndCreateTable();
                    break;
                default:
                    createNewTableIfDoesNotExistInDatabase();
                    break;
            }
        } catch (final Exception e) {
            throw new BpsRuntimeException(e.getMessage());
        }
    }

    /**
     * Gets the persisting mode.
     *
     * @param mode
     *            writing mode
     * @return the mode
     */
    private SaveMode getMode(final String mode) {

        LOGGER.trace("Entering the getMode method");

        if (mode == null) {
            return Append;
        } else if (Overwrite.name().equalsIgnoreCase(mode)) {
            return SaveMode.Overwrite;
        } else if (Append.name().equalsIgnoreCase(mode)) {
            return Append;
        }
        LOGGER.trace("Exiting the getMode method");

        return Append;
    }

    /**
     * Creates a single instance of the data base connection and uses through out source to sink flow in order to open connection, reads records and
     * write them in jdbc data base
     */

    public void prepareDataBaseConnection() {
        dataBaseConnection = DataBaseConnection.getInstance(drivername, username, password, dbURL);
        if (connection == null) {
            connection = dataBaseConnection.createDataBaseConnection();
        }

    }

    /**
     * To get resultSetMetadata for the specific table in JDBC database
     *
     * @return Returns ResultSetMetadata in order to get table schema
     * @throws ClassNotFoundException
     *             throws ClassNotFoundException
     * @throws SQLException
     *             throws SqlException
     */

    public ResultSetMetaData getResultSetMetaData() throws ClassNotFoundException, SQLException {
        final DatabaseMetaData databaseMetadata = connection.getMetaData();
        resultSet = databaseMetadata.getTables(null, null, tableName, null);
        while (resultSet.next()) {
            if (resultSet.getString("TABLE_NAME").equals(tableName)) {
                resultSet = connection.prepareStatement("SELECT * FROM " + tableName + " WHERE 1=0").executeQuery();
                return resultSet.getMetaData();
            } else {
                LOGGER.info("Table " + tableName + "not found in JDBC database");
                throw new BpsRuntimeException("Table " + tableName + "not found in JDBC database");
            }
        }
        LOGGER.error("Unable to get ResultSet from dataBase for table: " + tableName + " Please make sure table exists in" + " Database.");

        return null;
    }

    private void closeResultSet(final ResultSet resultSet) {
        try {
            if (resultSet != null) {
                resultSet.close();
            }
        } catch (final SQLException e) {
            LOGGER.error("SQL Exception occurred while closing the Result set :" + e);
        }
    }

    /**
     * Create a new table in jdbc database
     *
     * @throws ClassNotFoundException
     *             throws ClassNotFoundException
     * @throws SQLException
     *             throws sqlException
     */

    private void createNewTableIfDoesNotExistInDatabase() {

        try {
            if (!ifTableExistsInDataBase()) {
                for (final Map.Entry<String, String> entry : schemaMap.entrySet()) {
                    columnNames.add(entry.getKey());
                    columnTypes.add(entry.getValue());
                }
                createTable(tableName, columnNames, columnTypes);

            }
        } catch (final SQLException e) {
            LOGGER.error("SQL Exception occurred " + e.getMessage());
            throw new BpsRuntimeException(e);
        }
    }

    /**
     * To check if table exists in jdbc database and if it does then returns true or false
     *
     * @return true if table exists false if does not exist
     * @throws ClassNotFoundException
     *             throws ClassNotFoundException
     * @throws SQLException
     *             throws SqlException
     */
    private boolean ifTableExistsInDataBase() throws SQLException {
        final DatabaseMetaData databaseMetadata = connection.getMetaData();
        resultSet = databaseMetadata.getTables(null, null, tableName, null);
        while (resultSet.next()) {
            return resultSet.getString("TABLE_NAME").equals(tableName);
        }
        return false;
    }

    /**
     * to create table in jdbc database
     *
     * @param tableName
     *            Name of the table to be created in jdbc database
     * @param columnNames
     *            Name of the columns for table
     * @param columnTypes
     *            Data types for columns
     * @throws SQLException
     *             throws SqlException if execution of the query fails on database
     */

    @SuppressWarnings("PMD.CloseResource")
    private void createTable(final String tableName, final List<String> columnNames, final List<String> columnTypes) throws SQLException {
        Statement statement = null;
        try {
            statement = connection.createStatement();
            final StringBuilder sqlQueryBuilder = new StringBuilder("CREATE TABLE IF NOT EXISTS " + tableName + " (");
            for (int i = 0; i < columnNames.size(); i++) {
                sqlQueryBuilder.append(columnNames.get(i) + " " + columnTypes.get(i));
                sqlQueryBuilder.append(",");
            }
            final String createSqlStatement = sqlQueryBuilder.substring(0, sqlQueryBuilder.length() - 1) + ")";
            LOGGER.info("Sql Statement to create table :" + createSqlStatement);
            statement.executeUpdate(createSqlStatement);
        } finally {
            closeStatement(statement);
        }
    }

    private void closeStatement(final Statement statement) {
        try {
            if (statement != null) {
                statement.close();
            }
        } catch (final SQLException e) {
            LOGGER.error("Can not close Statement because of the sql Exception thrown " + e);
        }
    }

    /**
     * In order to overwrite table in JDBC database first table needs to be dropped and needs to be re-created new table. This method will drop the
     * table first and with the same schema a new table will be created.
     *
     * @throws ClassNotFoundException
     *             throws ClassNotFoundException
     * @throws SQLException
     *             throws SqlException
     */

    private void dropAndCreateTable() {

        try {
            dropTable(tableName);
            for (final Map.Entry<String, String> entry : schemaMap.entrySet()) {
                columnNames.add(entry.getKey());
                columnTypes.add(entry.getValue());
            }
            createTable(tableName, columnNames, columnTypes);

        } catch (final SQLException e) {
            throw new BpsRuntimeException(e);
        }

    }

    /**
     * Finally close the jdbc database connection.
     */

    public void closeConnection() {
        try {
            if (null != connection) {
                dataBaseConnection.cleanUpDatabaseConnectionInstance();
                connection.close();
            }

        } catch (final Exception e) {
            LOGGER.error("Exception occurred during closing data base connection : " + e.getMessage());
            throw new BpsRuntimeException(e);
        }
    }

    @SuppressWarnings("PMD.CloseResource")
    private void dropTable(final String tableName) throws SQLException {
        Statement statement = null;
        try {
            statement = connection.createStatement();
            final String sqlStatement = "DROP TABLE IF EXISTS " + tableName;
            statement.executeUpdate(sqlStatement);
        } finally {
            closeStatement(statement);
        }

    }

    /**
     * Constructs an insert sqlQuery to be performed on the jdbc data base in order to create table.
     *
     * @return an insert sqlQuery as String
     */
    public String getSqlQuery() {
        final List<String> columnNames = getColumnNames();
        final StringBuilder columns = new StringBuilder();
        final StringBuilder values = new StringBuilder();
        for (int i = 0; i < columnNames.size(); i++) {
            columns.append(columnNames.get(i));
            values.append("?");
            columns.append(",");
            values.append(",");
        }
        return "insert into " + tableName + "  (" + columns.substring(0, columns.length() - 1) + ") values ("
                + values.substring(0, values.length() - 1) + ")";
    }

    /**
     * To get column names from jdbc data base for the specific table specified in flow.xml
     *
     * @return list of column name from specific table as strings
     */

    public List<String> getColumnNames() {

        final List<String> columns = new LinkedList<String>();
        try {
            final ResultSetMetaData resultSetMetaData = getResultSetMetaData();
            final int numberCols = resultSetMetaData.getColumnCount();
            for (int i = 1; i <= numberCols; i++) {
                columns.add(resultSetMetaData.getColumnLabel(i));
            }
        } catch (final SQLException e) {
            LOGGER.error("SQL Exception occurred " + e.getMessage());
            throw new BpsRuntimeException(e);
        } catch (final Exception e) {
            LOGGER.error("Exception occurred " + e.getMessage());
            throw new BpsRuntimeException(e);
        } finally {
            closeConnection();
        }
        return columns;
    }

    /**
     * Property builder class to set parameters to database handler in order to suppoert database related operations
     */
    public static class DataBaseOperationsPropertyBuilder {

        private final DataBaseOperations dataBaseOperations;

        /**
         * Constructor to create new object for DataBaseOperations
         */
        public DataBaseOperationsPropertyBuilder() {
            this.dataBaseOperations = new DataBaseOperations();
        }

        /**
         * set userName to DataBaseOperations
         *
         * @param username
         *            user name to database
         * @return username
         */

        public DataBaseOperationsPropertyBuilder setUsername(final String username) {
            dataBaseOperations.username = username;
            return this;
        }

        /**
         * set password to DataBaseOperations
         *
         * @param password
         *            password of jdbc database
         * @return password
         */

        public DataBaseOperationsPropertyBuilder setPassword(final String password) {
            dataBaseOperations.password = password;
            return this;
        }

        /**
         * set driver name to DataBaseOperations
         *
         * @param drivername
         *            driver name of database
         * @return driverName
         */

        public DataBaseOperationsPropertyBuilder setDrivername(final String drivername) {
            dataBaseOperations.drivername = drivername;
            return this;
        }

        /**
         * set database url to DataBaseOperations
         *
         * @param dbURL
         *            url of the database
         * @return dbUrl
         */

        public DataBaseOperationsPropertyBuilder setDBUrl(final String dbURL) {
            dataBaseOperations.dbURL = dbURL;
            return this;
        }

        /**
         * set table name to DataBaseOperations
         *
         * @param tableName
         *            name of the table specified in flow.xml
         * @return tableName
         */

        public DataBaseOperationsPropertyBuilder setTableName(final String tableName) {
            dataBaseOperations.tableName = tableName;
            return this;
        }

        /**
         * set sql query to DataBaseOperations
         *
         * @param query
         *            sql query to perform on database
         * @return query
         */

        public DataBaseOperationsPropertyBuilder setQuery(final String query) {
            dataBaseOperations.query = query;
            return this;
        }

        /**
         * set Database schema for the table to be created in database
         *
         * @param schemaMap
         *            ColumnNames and ColumnsTypes
         * @return Returns database schema
         */
        public DataBaseOperationsPropertyBuilder setSchema(final Map<String, String> schemaMap) {
            dataBaseOperations.schemaMap = schemaMap;
            return this;
        }

        /**
         * To make sure necessary database parameters are specified during database operation in flow.xml
         *
         * @return DataBaseOperations
         */

        public DataBaseOperations build() {
            if (dataBaseOperations.username == null) {
                LOGGER.info("Username was not supplied separately.");
            }
            if (dataBaseOperations.password == null) {
                LOGGER.info("Password was not supplied separately.");
            }
            if (dataBaseOperations.dbURL == null) {
                throw new IllegalArgumentException("No database URL supplied");
            }
            if (dataBaseOperations.query == null) {
                throw new IllegalArgumentException("No query supplied");
            }
            if (dataBaseOperations.drivername == null) {
                throw new IllegalArgumentException("No driver supplied");
            }
            if (dataBaseOperations.tableName == null) {
                LOGGER.debug("No table Name specified.");
            }
            if (dataBaseOperations.schemaMap == null) {
                LOGGER.debug("No table schema specified.");
            }
            return dataBaseOperations;
        }
    }
}
