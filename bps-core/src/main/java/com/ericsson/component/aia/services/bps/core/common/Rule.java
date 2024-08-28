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
package com.ericsson.component.aia.services.bps.core.common;

/**
 * Enum class bps rule's context
 */
public enum Rule {

    URI("uri-rule"), FILE_URI("file-uri-rule"), ATTRIBUTE("attribute-rule");

    private String value;

    /**
     * Constructor
     *
     * @param value
     *            of enum
     */
    Rule(final String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
