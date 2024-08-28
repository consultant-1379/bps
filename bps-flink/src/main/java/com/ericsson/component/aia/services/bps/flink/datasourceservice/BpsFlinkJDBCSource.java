package com.ericsson.component.aia.services.bps.flink.datasourceservice;

import java.util.Map;

import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

/**
 * Sets Data stream of type {@link Row } Row and type Information which will be used by the user to get the schema in sample application.
 */
public class BpsFlinkJDBCSource {

    private DataStream<Row> dataStream;

    private RowTypeInfo rowTypeInfo;

    private Map<String, String> databaseSchema;

    /**
     * Get RowType Information
     *
     * @return returns row type information
     */
    public RowTypeInfo getRowTypeInfo() {
        return rowTypeInfo;
    }

    /**
     * Sets Row Type information
     *
     * @param rowTypeInfo
     *            rowtypeinfo
     */
    public void setRowTypeInfo(final RowTypeInfo rowTypeInfo) {
        this.rowTypeInfo = rowTypeInfo;
    }

    /**
     * Gets Data stream of type Row
     *
     * @return returns data Stream
     */
    public DataStream<Row> getDataStream() {
        return dataStream;
    }

    /**
     * sets Data Stream of Type Row
     *
     * @param dataStream
     *            sets data stream
     */
    public void setDataStream(final DataStream<Row> dataStream) {
        this.dataStream = dataStream;
    }

    /**
     * Get Data base Schema
     *
     * @return Returns Map of strings which contains Column Names and Column Types
     */
    public Map<String, String> getDatabaseSchema() {
        return databaseSchema;
    }

    /**
     * Sets Data base Schema
     *
     * @param databaseSchema
     *            Map of Strings of column names and types
     */
    public void setDatabaseSchema(final Map<String, String> databaseSchema) {
        this.databaseSchema = databaseSchema;
    }
}
