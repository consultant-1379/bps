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

import java.util.Properties;

import org.apache.flink.streaming.util.serialization.AbstractDeserializationSchema;

/**
 * This interface should be implemented by all the flink kafka deserialization schema implementation's.
 *
 * @param <O>
 *            output type of the flink kafka deserialization schema implementation
 */
public abstract class FlinkDeserializationSchema<O> extends AbstractDeserializationSchema<O> {

    private static final long serialVersionUID = 8930344373425251878L;

    /**
     * This method is used to set properties required by flink kafka deserialization schema implementation.required by flink kafka deserialization
     * schema implementation
     *
     * @param properties
     *            required by flink kafka deserialization schema implementation
     */
    public abstract void setProperties(Properties properties);
}