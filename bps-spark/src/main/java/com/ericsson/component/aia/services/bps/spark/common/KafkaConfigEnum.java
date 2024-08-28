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
package com.ericsson.component.aia.services.bps.spark.common;

/**
 * The Enum KafkaConfigEnum holds input source Kafka configurations.
 */
public enum KafkaConfigEnum {

    /** The kafka value decoder class. */
    VALUE_DECODER_CLASS("valueDecoder.class"),
    /** The kafka key decoder class. */
    KEY_DECODER_CLASS("keyDecoder.class"),
    /** The kafka value class. */
    VALUE_CLASS("valueClass"),
    /** The kafka key class. */
    KEY_CLASS("keyClass"),
    /** The topic. */
    TOPIC("topics"),
    /** The kafka value serializer. */
    VALUE_SERIALIZER("value.serializer"),
    /** The kafka key serializer. */
    KEY_SERIALIZER("key.serializer");

    /** The configuration. */
    public String configuration;

    /**
     * Instantiates a new kafka config enum.
     *
     * @param configuration
     *            the configuration
     */
    KafkaConfigEnum(final String configuration) {
        this.configuration = configuration;
    }

    /**
     * Gets the configuration.
     *
     * @return the configuration
     */
    public String getConfiguration() {
        return configuration;
    }
}
