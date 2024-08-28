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

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;

import com.ericsson.component.aia.services.bps.core.common.uri.IOURIS;
import com.ericsson.component.aia.services.bps.core.service.streams.BpsInputStreams;
import com.ericsson.component.aia.services.bps.core.service.streams.BpsStream;

/**
 * To get data stream of generic records and performs transformation of the records and returns POJO as records to write on kafka
 *
 * @param <T>
 *            GenericRecords {@link GenericRecord} from kafka to process data from stream
 */
public class SampleFlinkKafkaApp<T> implements SampleFlinkApp {

    @Override
    public DataStream<?> execute(final BpsInputStreams bpsInputStreams) {

        final String uri = (String) bpsInputStreams.getStreams("input-stream").getProperties().get("uri");
        final String dataFormat = uri.substring(uri.indexOf("?") + 1);
        DataStream datastream = null;

        if ("format=avro".equals(dataFormat)) {
            datastream = executeForAvroFormat(bpsInputStreams);
        } else if ("format=json".equals(dataFormat)) {
            datastream = executeForJsonFormat(bpsInputStreams);
        }

        return datastream;
    }

    @SuppressWarnings("PMD")
    private DataStream<POJO> executeForAvroFormat(final BpsInputStreams bpsInputStreams) {

        final BpsStream<DataStream<GenericRecord>> flinkStream = bpsInputStreams.<DataStream<GenericRecord>> getStreams("input-stream");
        DataStream<GenericRecord> dataStream = flinkStream.getStreamRef();

        //Some transformation
        final DataStream<POJO> pojo = dataStream.map(new MapFunction<GenericRecord, POJO>() {
            private static final long serialVersionUID = 3961882164579010828L;

            @Override
            @SuppressWarnings("PMD.SignatureDeclareThrowsException")
            public POJO map(final GenericRecord genericRecord) throws Exception {
                final POJO pojo = new POJO();
                final java.lang.reflect.Field[] typeFields = pojo.getClass().getDeclaredFields();
                final Map<String, java.lang.reflect.Field> typeFieldNames = new HashMap<String, java.lang.reflect.Field>();
                for (int index = 0; index < typeFields.length; index++) {
                    typeFields[index].setAccessible(true);
                    typeFieldNames.put(typeFields[index].getName(), typeFields[index]);
                }
                for (final Schema.Field field : genericRecord.getSchema().getFields()) {
                    Object col = genericRecord.get(field.name());
                    if (col != null && col instanceof Utf8) {
                        col = col.toString();
                    }
                    if (typeFieldNames.containsKey(field.name())) {
                        typeFieldNames.get(field.name()).set(pojo, col);
                    }
                }
                return pojo;
            }
        });
        return pojo;
    }

    private DataStream<?> executeForJsonFormat(final BpsInputStreams bpsInputStreams) {

        final BpsStream<DataStream<PositionEvent>> flinkStream = bpsInputStreams.<DataStream<PositionEvent>> getStreams("input-stream");
        final DataStream<PositionEvent> dataStream = flinkStream.getStreamRef();

        //Some transformation for tests
        final DataStream<PositionEvent> pojoStream = dataStream.map(new RichMapFunction<PositionEvent, PositionEvent>() {

            private static final long serialVersionUID = 6353350147171290068L;

            @Override
            public PositionEvent map(final PositionEvent value) {

                return value;
            }
        });
        return pojoStream;
    }

    @Override
    public String getServiceContextName() {
        return IOURIS.KAFKA.getUri();
    }
}
