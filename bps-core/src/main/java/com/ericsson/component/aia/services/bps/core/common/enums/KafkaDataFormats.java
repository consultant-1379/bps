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
package com.ericsson.component.aia.services.bps.core.common.enums;

/**
 * The Enum KafkaDataFormats.
 */
public enum KafkaDataFormats {

    /** The avro. */
    AVRO("avro"),
    /** The json. */
    JSON("json");

    /** The option. */
    public String dataFormat;

    /**
     * Instantiates a new spark csv enum.
     *
     * @param dataFormat
     *            the data format
     */
    KafkaDataFormats(final String dataFormat) {
        this.dataFormat = dataFormat;
    }

    /**
     * Gets the option.
     *
     * @return the option
     */
    public String getDataFormat() {
        return dataFormat;
    }

}
