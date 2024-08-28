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
package com.ericsson.component.aia.services.bps.flink.datasinkservice;

import java.util.Map;
import java.util.Properties;

import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.services.bps.core.common.Constants;
import com.ericsson.component.aia.services.bps.core.common.URIDefinition;
import com.ericsson.component.aia.services.bps.core.common.uri.IOURIS;
import com.ericsson.component.aia.services.bps.core.service.configuration.datasink.BpsDataSinkConfiguration;
import com.ericsson.component.aia.services.bps.core.service.streams.BpsAbstractDataSink;
import com.ericsson.component.aia.services.bps.flink.kafka.encoder.DataStreamToRow;
import com.ericsson.component.aia.services.bps.flink.utils.DataBaseOperations;

/**
 * The <code>BpsFlinkJdbcDataSink</code> class is responsible for writing {@link DataStream } to a Jdbc.<br>
 *
 * @param <T>
 *            the generic type representing the context like {@link StreamExecutionEnvironment} etc.
 */
public class BpsFlinkJdbcDataSink<T> extends BpsAbstractDataSink<StreamExecutionEnvironment, DataStream<?>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BpsFlinkJdbcDataSink.class);

    private static final String DEFAUT_QUERY = "sqlQuery";

    private DataBaseOperations dataBaseOperations;

    private String uri;

    private String driver;

    private String user;

    private String password;

    private String saveMode;

    private String query;

    private String tableName;

    private Map<String, String> databaseSchemaMap;

    private String outPutSchema;

    private int[] sqlSchemaTypeArray;

    /**
     * Configured instance of {@link BpsFlinkFileDataSink} for the specified sinkContextName.<br>
     * sinkContextName is the name of the output sink which was configured in flow.xml through tag &lt;output name="sinkContextName"&gt; ....
     * &lt;/output&gt;
     *
     * @param context
     *            the context for which Sink needs to be configured.
     * @param bpsDataSinkConfiguration
     *            the bps data sink configuration
     */
    @Override
    public void configureDataSink(final StreamExecutionEnvironment context, final BpsDataSinkConfiguration bpsDataSinkConfiguration) {
        super.configureDataSink(context, bpsDataSinkConfiguration);
        final Properties properties = bpsDataSinkConfiguration.getDataSinkConfiguration();
        final String sinkContextName = bpsDataSinkConfiguration.getDataSinkContextName();

        LOGGER.trace("ConfigureDataSink for context={} ", sinkContextName);
        uri = properties.getProperty(Constants.URI);
        driver = properties.getProperty(Constants.DRIVER);
        user = properties.getProperty(Constants.USER);
        password = properties.getProperty(Constants.PASSWORD);
        saveMode = properties.getProperty(Constants.SAVE_MODE);
        tableName = properties.getProperty(Constants.TABLE);
        outPutSchema = properties.getProperty(Constants.OUTPUT_SCHEMA);
        databaseSchemaMap = getDatabaseSchemaMap();
        sqlSchemaTypeArray = DataBaseOperations.getSqlInfoTypeArray(databaseSchemaMap);
        query = properties.getProperty(Constants.QUERY);
        if (null == query) {
            query = DEFAUT_QUERY;
            properties.setProperty(Constants.QUERY, query);
        }
        final URIDefinition<IOURIS> decode = IOURIS.decode(properties);
        uri = decode.getContext();
        dataBaseOperations = getDatabaseOperationsBuilder();

    }

    protected Map<String, String> getDatabaseSchemaMap() {
        return DataBaseOperations.convertsToDataBaseSchemaMap(outPutSchema);
    }

    /**
     * Writes Data to jdbc database
     *
     * @param dataStream
     *            bps data stream to be written in jdbc database
     */

    @Override
    public void write(final DataStream<?> dataStream) {
        LOGGER.info("Initiating Write for {}", getDataSinkContextName());

        if (dataStream == null) {
            LOGGER.info("Record null returning...");
            return;
        }
        DataStream sinkStream = dataStream;
        dataBaseOperations.tableOperations(saveMode);
        final String sqlQuery = dataBaseOperations.getSqlQuery();
        sinkStream = getRowDataStream(dataStream);
        final JDBCOutputFormat jdbcOutputFormat = JDBCOutputFormat.buildJDBCOutputFormat().setDrivername(driver).setUsername(user)
                .setPassword(password).setQuery(sqlQuery).setBatchInterval(0).setSqlTypes(sqlSchemaTypeArray).setDBUrl(uri).finish();
        sinkStream.writeUsingOutputFormat(jdbcOutputFormat);
        LOGGER.info("BpsFlinkJdbcDataSink [Context=" + getDataSinkContextName() + "] write operation completed successfully.");

    }

    /**
     * converts the data stream coming from kafka or jdbc data base to Row dataStream in order to write the records in jdbc data base
     *
     * @param dataStream
     *            data streams coming from kafka or jdbc
     * @return returns Row data stream for the jdbc output format.
     */
    protected DataStream<T> getRowDataStream(final DataStream<?> dataStream) {
        return (DataStream<T>) DataStreamToRow.transformRecordsInRow(dataStream, databaseSchemaMap);
    }

    protected DataBaseOperations getDatabaseOperationsBuilder() {
        return DataBaseOperations.buildPropertyBuilder().setDrivername(driver).setUsername(user).setPassword(password).setDBUrl(uri)
                .setTableName(tableName).setQuery(query).setSchema(databaseSchemaMap).build();
    }

    protected void setDataBaseOperations(final DataBaseOperations dataBaseOperations) {
        this.dataBaseOperations = dataBaseOperations;
    }

    protected void setOutPutSchema(final String outPutSchema) {
        this.outPutSchema = outPutSchema;
    }

    @Override
    public void cleanUp() {
        LOGGER.trace("Cleaned resources allocated for {} ", getDataSinkContextName());
    }

    /**
     * Used to get jdbc service context name
     *
     * @return JDBC://
     */
    @Override
    public String getServiceContextName() {
        return IOURIS.JDBC.getUri();
    }
}
