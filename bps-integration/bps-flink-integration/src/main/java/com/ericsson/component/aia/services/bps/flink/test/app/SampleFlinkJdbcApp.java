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
package com.ericsson.component.aia.services.bps.flink.test.app;

import java.util.Map;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import com.ericsson.component.aia.services.bps.core.common.uri.IOURIS;
import com.ericsson.component.aia.services.bps.core.service.streams.BpsInputStreams;
import com.ericsson.component.aia.services.bps.flink.datasourceservice.BpsFlinkJDBCSource;

/**
 * Returns Data Streams to write data into JDBC database
 *
 * @param <T>
 *            Data stream type to write data into jdbc database
 */

public class SampleFlinkJdbcApp<T> implements SampleFlinkApp {

    @Override
    public DataStream<?> execute(final BpsInputStreams bpsInputStreams) {
        final BpsFlinkJDBCSource bpsFlinkJDBCSource = (BpsFlinkJDBCSource) (bpsInputStreams.getStreams("input-stream").getStreamRef());
        final RowTypeInfo rowTypeInfo = bpsFlinkJDBCSource.getRowTypeInfo();
        final Map<String, String> dataBaseSchema = bpsFlinkJDBCSource.getDatabaseSchema();
        rowTypeInfo.getTotalFields();
        final DataStream<Row> recordDataStream = bpsFlinkJDBCSource.getDataStream().map(new MapFunction<Row, Row>() {
            private static final long serialVersionUID = 3961882164579010828L;

            @Override
            @SuppressWarnings("PMD.SignatureDeclareThrowsException")
            public Row map(final Row row) throws Exception {
                for (int i = 0; i < row.getArity(); i++) {
                    final TypeInformation typeInformation = rowTypeInfo.getTypeAt(i);
                    final Class<T> type = typeInformation.getTypeClass();
                    if (row.getField(i).getClass().equals(type)) {
                        for (final Map.Entry<String, String> entry : dataBaseSchema.entrySet()) {
                            if (entry.getKey().equals("ID")) {
                                row.getField(i);
                            }
                        }
                    }
                }
                return row;
            }
        });
        return recordDataStream;
    }

    @Override
    public String getServiceContextName() {
        return IOURIS.JDBC.getUri();
    }
}
