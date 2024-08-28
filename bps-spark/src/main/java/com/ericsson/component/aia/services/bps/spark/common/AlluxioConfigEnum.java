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
 * Enum for Alluxio configurations.
 */
public enum AlluxioConfigEnum {

    /** The bean class. */
    BEAN_CLASS("bean.class"),

    SAVE_AS_OBJECT("saveAsObject");

    /** The configuration. */
    public String configuration;

    /**
     * Instantiates a new kafka config enum.
     *
     * @param configuration
     *            the configuration
     */
    AlluxioConfigEnum(final String configuration) {
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