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

import java.io.IOException;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Deserialization schema class that converts a json string to {@code T}
 *
 * @param <T>
 */
public class FlinkKafkaJsonDeserializationSchema implements DeserializationSchema<PositionEvent> {

    private static final long serialVersionUID = -1069677209879171979L;

    private ObjectMapper mapper;

    @Override
    public TypeInformation<PositionEvent> getProducedType() {
        return TypeInformation.of(PositionEvent.class);
    }

    @Override
    public PositionEvent deserialize(final byte[] message) throws IOException {
        if (null == mapper) {
            mapper = new ObjectMapper();
        }
        final String str = new String(message);
        return mapper.readValue(str, PositionEvent.class);
    }

    @Override
    public boolean isEndOfStream(final PositionEvent nextElement) {
        return false;
    }

}
