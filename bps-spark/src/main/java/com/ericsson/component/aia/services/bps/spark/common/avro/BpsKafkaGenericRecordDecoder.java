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
package com.ericsson.component.aia.services.bps.spark.common.avro;

import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.common.avro.decoder.DefaultAvroClient;
import com.ericsson.component.aia.common.avro.decoder.GenericRecordDecoder;

import kafka.serializer.Decoder;
import kafka.utils.VerifiableProperties;

/**
 * The Class BpsKafkaGenericRecordDecoder. A wrapper for a GenericRecordDecoder which will allow Kafka to use the decoder internally as a deserializer
 * class. The methods configure() and close() are left unimplemented, as is the standard industry practice when implementing Kafka Serializers. The
 * deserialize() method is similarly boiler plate code we must implement, even though it won't be used.
 */
public class BpsKafkaGenericRecordDecoder implements Decoder<GenericRecord>, Deserializer<GenericRecord> {

    protected static final Logger LOGGER = LoggerFactory.getLogger(BpsKafkaGenericRecordDecoder.class);

    /** The decoder. */
    private final GenericRecordDecoder decoder;

    /**
     * Instantiates a new kafka generic record decoder.
     */
    public BpsKafkaGenericRecordDecoder() {
        this.decoder = new GenericRecordDecoder();
    }

    /**
     * Instantiates a new kafka generic record decoder.Even though this constructor takes an argument and does nothing with it, this exact constructor
     * is require for Kafka serializers to compile.
     *
     * @param props
     *            the props
     */
    public BpsKafkaGenericRecordDecoder(final VerifiableProperties props) {
        this.decoder = new GenericRecordDecoder(new DefaultAvroClient(props.props()));
    }

    /**
     * Instantiates a new kafka generic record decoder.
     *
     * @param decoder
     *            the decoder
     */
    public BpsKafkaGenericRecordDecoder(final GenericRecordDecoder decoder) {
        this.decoder = decoder;
    }

    /**
     * From bytes.
     *
     * @param data
     *            the data
     * @return the generic record
     */
    @Override
    public GenericRecord fromBytes(final byte[] data) {
        return this.decoder.decode(data);
    }

    /**
     * Deserialize.
     *
     * @param topic
     *            the topic
     * @param data
     *            the data
     * @return the generic record
     */
    @Override
    public GenericRecord deserialize(final String topic, final byte[] data) {
        return this.decoder.decode(data);
    }

    /**
     * Configure.
     *
     * @param configs
     *            the configs
     * @param isKey
     *            the is key
     */
    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        LOGGER.info("'Configure' method was invoked but it hasn't been implemented for {}", this.getClass().getSimpleName());
    }

    /**
     * Close.
     */
    @Override
    public void close() {
        LOGGER.info("'Close' method was invoked but it hasn't been implemented for {}", this.getClass().getSimpleName());
    }
}
