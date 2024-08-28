package com.ericsson.component.aia.services.bps.flink.utils;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * Tests the behaviour of DataBase Operations such as create, delete tables as well as getting data type and column name For table to be created and
 * deleted based on the write mode selected in flow file.
 */
@RunWith(MockitoJUnitRunner.class)
public class DataBaseOperationsTest {

    @Spy
    private DataBaseOperations dataBaseOperations;

    @Mock
    private DataBaseConnection dataBaseConnection;

    @Mock
    private Connection connection;

    @Mock
    private DatabaseMetaData databaseMetaData;

    @Mock
    private ResultSetMetaData resultSetMetaData;

    @Mock
    private ResultSet resultSet;

    @Mock
    private PreparedStatement preparedStatement;

    @Mock
    private Statement statement;

    private Map<String, String> schema = new HashMap<String, String>();

    private String dbUrl = "jdbc:h2:/";

    private String user = "test";

    private String password = "test";

    private String driver = "org.h2.driver";

    private String INSERT_QUERY = "insert into table  (ColumnName) values (?)";

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        schema.put("columnName", "String");
        dataBaseOperations = DataBaseOperations.buildPropertyBuilder().setDrivername(driver).setUsername(user).setPassword(password).setSchema(schema)
                .setDBUrl(dbUrl).setTableName("table").setQuery("").build();
    }

    @Test
    public void testGetRawTypeInfo() throws Exception {
        dataBaseOperations.setConnection(connection);
        dataBaseOperations.setTableName("table");
        dataBaseOperations.prepareDataBaseConnection();
        getResultMetaData();
        Assert.assertTrue(dataBaseOperations.getRawTypeInfo() != null);
    }

    private void getResultMetaData() throws SQLException {
        when(connection.getMetaData()).thenReturn(databaseMetaData);
        when(databaseMetaData.getTables(null, null, "table", null)).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getString("TABLE_NAME")).thenReturn("table");
        when(connection.prepareStatement("SELECT * FROM table WHERE 1=0")).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.getMetaData()).thenReturn(resultSetMetaData);
        when(resultSetMetaData.getColumnCount()).thenReturn(1);
        when(resultSetMetaData.getColumnClassName(1)).thenReturn(anyString().getClass().getCanonicalName());
        when(resultSetMetaData.getColumnLabel(1)).thenReturn("ColumnName");
        when(resultSetMetaData.getColumnTypeName(1)).thenReturn("ColumnType");
    }

    @Test
    public void testGetSqlQuery() throws Exception {
        dataBaseOperations.setConnection(connection);
        dataBaseOperations.setTableName("table");
        dataBaseOperations.prepareDataBaseConnection();
        getResultMetaData();
        final String insertQuery = dataBaseOperations.getSqlQuery();
        Assert.assertEquals(insertQuery, INSERT_QUERY);
    }

    @Test
    public void testSqlTypeInfo() {
        final int[] typeArray = DataBaseOperations.getSqlInfoTypeArray(schema);
        Assert.assertTrue(typeArray != null);
    }

    @Test
    public void testConvertsToDataBaseSchemaMap() throws Exception {
        final Map<String, String> schema = DataBaseOperations.convertsToDataBaseSchemaMap("com.ericsson.component.aia.services.bps.flink.utils.POJO");
        Assert.assertTrue(schema != null);
    }

}
