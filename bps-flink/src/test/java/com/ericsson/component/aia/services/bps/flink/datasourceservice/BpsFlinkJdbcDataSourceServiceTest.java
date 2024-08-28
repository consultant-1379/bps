package com.ericsson.component.aia.services.bps.flink.datasourceservice;

import static junit.framework.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;

import com.ericsson.component.aia.services.bps.core.common.Constants;
import com.ericsson.component.aia.services.bps.core.exception.BpsRuntimeException;
import com.ericsson.component.aia.services.bps.flink.utils.DataBaseConnection;
import com.ericsson.component.aia.services.bps.flink.utils.DataBaseOperations;

/**
 * Tests the behaviour of the BpsFlinkJdbcDataSink class.
 */
@RunWith(MockitoJUnitRunner.class)
public class BpsFlinkJdbcDataSourceServiceTest {

    @Spy
    private BpsFlinkJdbcDataSourceService bpsFlinkJdbcDataSourceService;

    @Mock
    private DataBaseConnection dataBaseConnection;

    @Mock
    private DataBaseOperations dataBaseOperations;

    @Mock
    private Connection connection;

    @Mock
    private StreamExecutionEnvironment streamExecutionEnvironment;

    @Mock
    private RowTypeInfo rowTypeInfo;

    @Mock
    private DataStreamSource<Row> dataStream;

    @Mock
    private JDBCInputFormat jdbcInputFormat;

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

    private Properties properties;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        properties = new Properties();
        properties.put(Constants.URI, "JDBC://jdbc:h2:/");
        properties.put(Constants.USER, "test");
        properties.put(Constants.PASSWORD, "test");
        properties.put(Constants.DRIVER, "org.h2.driver");
        properties.put(Constants.TABLE, "table");
        properties.put(Constants.QUERY, "SELECT * FROM table");

    }

    @Test
    public void testConfigureDataSource() throws Exception {
        bpsFlinkJdbcDataSourceService.setDataBaseOperations(dataBaseOperations);
        bpsFlinkJdbcDataSourceService.configureDataSource(streamExecutionEnvironment, properties, "inputContext");
        verify(bpsFlinkJdbcDataSourceService).configureDataSource(streamExecutionEnvironment, properties, "inputContext");
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
    public void testGetDataStream() throws Exception {
        bpsFlinkJdbcDataSourceService.configureDataSource(streamExecutionEnvironment, properties, "inputContext");
        when(bpsFlinkJdbcDataSourceService.getDatabaseBuilder()).thenReturn(dataBaseOperations);
        bpsFlinkJdbcDataSourceService.setDataBaseOperations(dataBaseOperations);
        dataBaseOperations.setConnection(connection);
        getResultMetaData();
        bpsFlinkJdbcDataSourceService.setRowTypeInfo(rowTypeInfo);
        when(bpsFlinkJdbcDataSourceService.getFlinkJdbcInputFormat()).thenReturn(jdbcInputFormat);
        when(streamExecutionEnvironment.createInput(jdbcInputFormat)).thenReturn(dataStream);
        assertEquals(true, bpsFlinkJdbcDataSourceService.getDataStream() != null);
    }

    @Test(expected = BpsRuntimeException.class)
    public void testExceptionWhileGetDataStream() throws Exception {
        bpsFlinkJdbcDataSourceService.configureDataSource(streamExecutionEnvironment, properties, "inputContext");
        dataBaseOperations = bpsFlinkJdbcDataSourceService.getDatabaseBuilder();
        bpsFlinkJdbcDataSourceService.setDataBaseOperations(dataBaseOperations);
        dataBaseOperations.setConnection(connection);
        doThrow(SQLException.class).when(connection).getMetaData();
        bpsFlinkJdbcDataSourceService.getDataStream();

    }

    @Test
    public void testGetServiceContextName() throws Exception {
        bpsFlinkJdbcDataSourceService.configureDataSource(streamExecutionEnvironment, properties, "inputContext");
        final String serviceContextName = bpsFlinkJdbcDataSourceService.getServiceContextName();
        assertEquals(serviceContextName, "jdbc://");
    }

}
