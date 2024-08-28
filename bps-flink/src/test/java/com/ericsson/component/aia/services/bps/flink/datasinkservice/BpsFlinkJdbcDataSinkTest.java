package com.ericsson.component.aia.services.bps.flink.datasinkservice;

import static junit.framework.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;

import com.ericsson.component.aia.itpf.common.modeling.flow.schema.gen.fbp_flow.SinkType;
import com.ericsson.component.aia.services.bps.core.common.Constants;
import com.ericsson.component.aia.services.bps.core.exception.BpsRuntimeException;
import com.ericsson.component.aia.services.bps.core.service.configuration.datasink.BpsDataSinkConfiguration;
import com.ericsson.component.aia.services.bps.core.service.configuration.datasink.DefaultBpsDataSinkConfiguration;
import com.ericsson.component.aia.services.bps.flink.utils.DataBaseConnection;
import com.ericsson.component.aia.services.bps.flink.utils.DataBaseOperations;

/**
 * Tests the behaviour of the BpsFlinkJdbcDataSink class.
 */
@RunWith(MockitoJUnitRunner.class)
public class BpsFlinkJdbcDataSinkTest {

    @Spy
    private BpsFlinkJdbcDataSink bpsFlinkJdbcDataSink;

    @Mock
    private DataStream dataStream;

    @Mock
    private StreamExecutionEnvironment streamExecutionEnvironment;

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

    private DataBaseOperations dataBaseOperations;

    private Properties properties;

    private String INSERT_QUERY = "insert into table  (ColumnName) values (?)";

    private Map<String, String> schemaMap = new HashMap<String, String>();
    private BpsDataSinkConfiguration bpsDataSinkConfiguration;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        properties = new Properties();
        properties.put(Constants.URI, "JDBC://jdbc:h2:/");
        properties.put(Constants.USER, "test");
        properties.put(Constants.PASSWORD, "test");
        properties.put(Constants.DRIVER, "org.h2.driver");
        properties.put(Constants.SAVE_MODE, "Append");
        properties.put(Constants.OUTPUT_SCHEMA, "com.ericsson.component.aia.services.bps.flink.utils.POJO");
        properties.put(Constants.TABLE, "table");
        properties.put(Constants.QUERY, "SELECT * FROM table");
        schemaMap.put("SCANNER_ID", "BIGINT");
        schemaMap.put("RBS_MODULE_ID", "BIGINT");
        bpsFlinkJdbcDataSink.setOutPutSchema(properties.getProperty(Constants.OUTPUT_SCHEMA));
        bpsDataSinkConfiguration = new DefaultBpsDataSinkConfiguration();
        bpsDataSinkConfiguration.configure("outputContext", properties, new ArrayList<SinkType>());
    }

    @Test
    public void testConfigureDataSink() {
        when(bpsFlinkJdbcDataSink.getDatabaseSchemaMap()).thenReturn(schemaMap);
        bpsFlinkJdbcDataSink.configureDataSink(streamExecutionEnvironment, bpsDataSinkConfiguration);
        verify(bpsFlinkJdbcDataSink).configureDataSink(streamExecutionEnvironment, bpsDataSinkConfiguration);

    }

    @Test
    public void testWriteDataStream() throws Exception {
        when(bpsFlinkJdbcDataSink.getDatabaseSchemaMap()).thenReturn(schemaMap);
        bpsFlinkJdbcDataSink.configureDataSink(streamExecutionEnvironment, bpsDataSinkConfiguration);
        dataBaseOperations = bpsFlinkJdbcDataSink.getDatabaseOperationsBuilder();
        bpsFlinkJdbcDataSink.setDataBaseOperations(dataBaseOperations);
        dataBaseOperations.setConnection(connection);
        getResultMetaData();
        doReturn(dataStream).when(bpsFlinkJdbcDataSink).getRowDataStream(dataStream);
        bpsFlinkJdbcDataSink.write(dataStream);
        verify(bpsFlinkJdbcDataSink).write(dataStream);

    }

    @Test(expected = BpsRuntimeException.class)
    public void testExceptionWhileWrite() throws Exception {
        when(bpsFlinkJdbcDataSink.getDatabaseSchemaMap()).thenReturn(schemaMap);
        bpsFlinkJdbcDataSink.configureDataSink(streamExecutionEnvironment, bpsDataSinkConfiguration);
        dataBaseOperations = bpsFlinkJdbcDataSink.getDatabaseOperationsBuilder();
        bpsFlinkJdbcDataSink.setDataBaseOperations(dataBaseOperations);
        bpsFlinkJdbcDataSink.write(dataStream);
    }

    @Test
    public void testGetServiceContextName() throws Exception {
        when(bpsFlinkJdbcDataSink.getDatabaseSchemaMap()).thenReturn(schemaMap);
        bpsFlinkJdbcDataSink.configureDataSink(streamExecutionEnvironment, bpsDataSinkConfiguration);
        final String serviceContextName = bpsFlinkJdbcDataSink.getServiceContextName();
        assertEquals(serviceContextName, "jdbc://");
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

}
