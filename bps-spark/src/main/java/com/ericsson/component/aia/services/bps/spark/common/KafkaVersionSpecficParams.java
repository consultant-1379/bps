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
 * The Enumeration {code}KafkaVersionSpecficParams{code} holds the Kafka version specific attributes. This enumeration can be removed when new version
 * specific validation strategy implemented as part of common flow validation.{@linkplain https://jira-nam.lmera.ericsson.se/browse/OSSBSS-1941}
 */
public enum KafkaVersionSpecficParams {
    /** The kafka metadata broker list. */
    BROKER_LIST_08("metadata.broker.list"),
    /** The broker list 09. */
    BROKER_LIST_09("bootstrap.servers");

    /** The configuration. */
    public String configuration;

    /**
     * Instantiates a new kafka config enum.
     *
     * @param configuration
     *            the configuration
     */
    KafkaVersionSpecficParams(final String configuration) {
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
