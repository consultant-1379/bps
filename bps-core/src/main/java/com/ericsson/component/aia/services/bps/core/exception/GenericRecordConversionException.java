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
package com.ericsson.component.aia.services.bps.core.exception;

/**
 * The <code>GenericRecordConversionException<code> represent the exception associated with conversion of generic record.
 */
public class GenericRecordConversionException extends Exception {

    private static final long serialVersionUID = 1L;

    /**
     * Default constructor.
     */
    public GenericRecordConversionException() {
        super();
    }

    /**
     * Constructor with error message.
     *
     * @param message
     *            : error message.
     */
    public GenericRecordConversionException(final String message) {
        super(message);
    }

}
