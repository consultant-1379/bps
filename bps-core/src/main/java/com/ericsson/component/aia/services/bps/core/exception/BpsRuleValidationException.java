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

import java.util.Collection;

/**
 * Runtime exception class for bps rule validation exception scenarios
 */
public class BpsRuleValidationException extends BpsRuntimeException {

    private static final long serialVersionUID = 6982690433572514841L;

    /**
     * Constructor accepts exception message
     *
     * @param message
     *            exception message
     */
    public BpsRuleValidationException(final String message) {
        super(message);
    }

    /**
     * Constructor accepts exception message and Throwable cause
     *
     * @param message
     *            exception message
     * @param cause
     *            Throwable cause
     */
    public BpsRuleValidationException(final String message, final Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructor accepts exception messages
     *
     * @param messages
     *            collection of exception messages
     */
    public BpsRuleValidationException(final Collection<String> messages) {
        super(messages.toString());
    }
}
