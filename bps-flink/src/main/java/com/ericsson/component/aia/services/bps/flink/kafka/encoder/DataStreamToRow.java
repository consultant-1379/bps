package com.ericsson.component.aia.services.bps.flink.kafka.encoder;

import java.util.Map;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import com.ericsson.component.aia.services.bps.core.exception.BpsRuntimeException;

/**
 * Converts the kafka data streams to row for jdbc data base
 */
public class DataStreamToRow {

    /**
     * Constructor
     */
    private DataStreamToRow() {
    }

    /**
     * Transform kafka data stream into jdbc row format
     *
     * @param dataStream
     *            Kafka DataStream
     * @param <T>
     *            type events
     * @param filesNameAndTypesMap
     *            Map of column names and types
     * @return row data stream for jdbc sink
     */
    public static <T> DataStream<Row> transformRecordsInRow(final DataStream<T> dataStream, final Map<String, String> filesNameAndTypesMap) {

        return dataStream.map(new MapFunction<T, Row>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Row map(final T type) {
                return convertRecordsInRow(type, filesNameAndTypesMap);
            }
        });
    }

    private static <T> Row convertRecordsInRow(final T type, final Map<String, String> avroSchemaFields) {
        final Row row = new Row(avroSchemaFields.size());
        if (type instanceof Row) {
            return (Row) type;
        } else {
            final java.lang.reflect.Field[] typeFields = type.getClass().getDeclaredFields();
            for (int i = 0; i < avroSchemaFields.size(); i++) {
                try {
                    final boolean isAccessible = typeFields[i].isAccessible();
                    if (!isAccessible) {
                        typeFields[i].setAccessible(true);
                    }
                    final Object object = typeFields[i].get(type);
                    row.setField(i, object);
                    typeFields[i].setAccessible(isAccessible);
                } catch (final IllegalAccessException e) {
                    throw new BpsRuntimeException("Failed to access Type Fields :" + e);
                }
            }
            return row;
        }
    }

}
