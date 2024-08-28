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
package com.ericsson.component.aia.services.bps.flink.datasourceservice;

import java.util.Map;
import java.util.Properties;

import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.services.bps.core.common.Constants;
import com.ericsson.component.aia.services.bps.core.common.URIDefinition;
import com.ericsson.component.aia.services.bps.core.common.uri.IOURIS;
import com.ericsson.component.aia.services.bps.core.datasourceservice.AbstractBpsDataSourceService;
import com.ericsson.component.aia.services.bps.flink.utils.DataBaseOperations;

/**
 * The <code>BpsFlinkJdbcDataSourceService</code> is responsible for reading data from jdbc database and return respectiveb# {@link DataStream}.
 *
 * The <code>BpsFlinkJdbcDataSourceService</code> implements <code>BpsDataSourceService&lt;StreamExecutionEnvironment,
 * DataStream&gt;</code> which is specific to StreamExecutionEnvironment and DataStream.
 */
public class BpsFlinkJdbcDataSourceService extends AbstractBpsDataSourceService<StreamExecutionEnvironment, BpsFlinkJDBCSource> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BpsFlinkJdbcDataSourceService.class);

    private static final String DEFAULT_QUERY = "SELECT * FROM ";

    private DataBaseOperations dataBaseOperations;

    private String uri;

    private String driver;

    private String user;

    private String password;

    private String tableName;

    private String query;

    private RowTypeInfo rowTypeInfo;

    private Map<String, String> schema;

    /**
     * Configured instance of {@link BpsFlinkJdbcDataSourceService} for the specified sinkContextName.<br>
     * sinkContextName is the name of the input source which was configured in flow.xml &lt;/output&gt;
     *
     * @param context
     *            the context for which source needs to be configured.
     * @param properties
     *            the configuration associated with underlying output sink.
     * @param dataSourceContextName
     *            Unique name associated with each of the output sink.
     */
    @Override
    public void configureDataSource(final StreamExecutionEnvironment context, final Properties properties, final String dataSourceContextName) {
        super.configureDataSource(context, properties, dataSourceContextName);
        uri = properties.getProperty(Constants.URI);
        driver = properties.getProperty(Constants.DRIVER);
        user = properties.getProperty(Constants.USER);
        password = properties.getProperty(Constants.PASSWORD);
        tableName = properties.getProperty(Constants.TABLE);
        query = properties.getProperty(Constants.QUERY);
        if (null == query) {
            query = DEFAULT_QUERY + tableName;
            properties.setProperty(Constants.QUERY, query);
        }
        final URIDefinition<IOURIS> decode = IOURIS.decode(properties);
        uri = decode.getContext();
        dataBaseOperations = getDatabaseBuilder();

    }

    /**
     * Used to get the DataStream based on the provided configurations in flow xml.
     *
     * @return data streams from jdbc database
     */
    @Override
    public BpsFlinkJDBCSource getDataStream() {
        LOGGER.trace("Entering the getJDBDataStreamMethod ");
        DataStream<Row> dataStream;
        rowTypeInfo = dataBaseOperations.getRawTypeInfo();
        schema = dataBaseOperations.getSchema();
        final JDBCInputFormat jdbcInputFormat = getFlinkJdbcInputFormat();
        dataStream = context.createInput(jdbcInputFormat);
        final BpsFlinkJDBCSource bpsFlinkJDBCSource = new BpsFlinkJDBCSource();
        bpsFlinkJDBCSource.setDataStream(dataStream);
        bpsFlinkJDBCSource.setRowTypeInfo(rowTypeInfo);
        bpsFlinkJDBCSource.setDatabaseSchema(schema);
        LOGGER.trace("Exiting the getJDBDataStreamMethod method ");
        return bpsFlinkJDBCSource;
    }

    protected JDBCInputFormat getFlinkJdbcInputFormat() {
        return JDBCInputFormat.buildJDBCInputFormat().setDrivername(driver).setDBUrl(uri).setUsername(user).setPassword(password).setQuery(query)
                .setRowTypeInfo(rowTypeInfo).finish();
    }

    /**
     * Get the database builder in order to help database to execute database Operations
     *
     * @return new instance of database operations
     */
    protected DataBaseOperations getDatabaseBuilder() {
        return DataBaseOperations.buildPropertyBuilder().setDrivername(driver).setUsername(user).setPassword(password).setDBUrl(uri)
                .setTableName(tableName).setQuery(query).build();
    }

    protected void setDataBaseOperations(final DataBaseOperations dataBaseOperations) {
        this.dataBaseOperations = dataBaseOperations;
    }

    protected void setRowTypeInfo(final RowTypeInfo rowTypeInfo) {
        this.rowTypeInfo = rowTypeInfo;
    }

    /**
     * Gets the service context name as defined in flow xml.
     *
     * @return the data stream
     */
    @Override
    public String getServiceContextName() {
        return IOURIS.JDBC.getUri();
    }
}