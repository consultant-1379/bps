package com.ericsson.component.aia.services.bps.flink.test.app;

import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.apache.flink.types.Row;

/**
 * Created by epunita on 3/10/17.
 */
public class FlinkKafkaJsonRowSerializationSchema implements SerializationSchema<Row> {

    @Override
    public byte[] serialize(final Row element) {
        try {
            return element.toString().getBytes();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

}