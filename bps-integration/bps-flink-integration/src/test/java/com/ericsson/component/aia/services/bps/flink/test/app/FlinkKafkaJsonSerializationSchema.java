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
package com.ericsson.component.aia.services.bps.flink.test.app;

import org.apache.flink.streaming.util.serialization.SerializationSchema;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Seralizes type {@code T} to json string
 *
 * @param <T>
 */
public class FlinkKafkaJsonSerializationSchema implements SerializationSchema<PositionEvent> {

    private static final long serialVersionUID = 8695587743493555271L;

    ObjectMapper mapper = new ObjectMapper();

    @Override
    public byte[] serialize(final PositionEvent element) {
        try {
            return mapper.writeValueAsString(element).getBytes();
        } catch (final JsonProcessingException e) {
            e.printStackTrace();
        }
        return null;
    }

}
