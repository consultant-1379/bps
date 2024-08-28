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
package com.ericsson.component.aia.services.bps.flink.kafka.encoder;

import org.apache.flink.streaming.util.serialization.SerializationSchema;

import com.ericsson.component.aia.common.avro.GenericRecordWrapper;
import com.ericsson.component.aia.common.avro.encoder.GenericRecordEncoder;

/**
 * This class is responsible to serialize kafka message and return byte array.
 */
public class FlinkKafkaGenericRecordSerializationSchema implements SerializationSchema<GenericRecordWrapper> {

    private static final long serialVersionUID = 1L;

    /** The encoder. */
    private final GenericRecordEncoder encoder = new GenericRecordEncoder();

    @Override
    public byte[] serialize(final GenericRecordWrapper genericRecord) {
        return encoder.encode(genericRecord);
    }
}