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
package com.ericsson.component.aia.services.bps.flink.kafka.decoder;

import java.io.IOException;
import java.util.Properties;

import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import com.ericsson.component.aia.common.avro.decoder.DefaultAvroClient;
import com.ericsson.component.aia.common.avro.decoder.GenericRecordDecoder;

/**
 * This class is responsible to deserialize kafka message and return {@link GenericRecord} with <String,Object> as key-value pair
 */
public class FlinkKafkaGenericRecordDeserializationSchema extends FlinkDeserializationSchema<GenericRecord> {

    private static final long serialVersionUID = 1L;

    private Properties properties;

    @Override
    public TypeInformation<GenericRecord> getProducedType() {
        return TypeInformation.of(GenericRecord.class);
    }

    @Override
    public GenericRecord deserialize(final byte[] encodedBytes) throws IOException {
        final GenericRecordDecoder decoder;
        if (properties == null) {
            decoder = new GenericRecordDecoder();
            return decoder.decode(encodedBytes);
        } else {
            decoder = new GenericRecordDecoder(new DefaultAvroClient(properties));
            return decoder.decode(encodedBytes);
        }
    }

    @Override
    public boolean isEndOfStream(final GenericRecord arg0) {
        return false;
    }

    @Override
    public void setProperties(final Properties properties) {
        this.properties = properties;
    }
}