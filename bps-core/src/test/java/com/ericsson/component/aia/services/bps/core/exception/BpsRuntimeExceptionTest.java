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

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * Validate Runtime exception class for bps exception scenarios
 */
public class BpsRuntimeExceptionTest {

    /**
     * Test BpsRuntimeException with specific message.
     */
    @Test
    public void testBpsRuleValidationException() {
        final BpsRuntimeException exception = new BpsRuntimeException("BPS-RULE-Validation-Exception");
        assertEquals("BPS-RULE-Validation-Exception", exception.getMessage());
    }

    /**
     * Test BpsRuntimeException with specific message and root cause exception object.
     */
    @Test
    public void testBpsRuleValidationExceptionWithExceptionObjectAndMessage() {
        final Exception rootCause = new Exception("BPS-RULE-Validation-Exception");
        final BpsRuntimeException exception = new BpsRuntimeException("BPS-RULE-Validation-Exception", rootCause);
        assertEquals("BPS-RULE-Validation-Exception", exception.getMessage());
        assertEquals(rootCause, exception.getCause());
    }

    /**
     * Test BpsRuntimeException with root cause.
     */
    @Test
    public void testBpsRuleValidationExceptionWithException() {
        final Exception rootCause = new Exception("BPS-RULE-Validation-Exception");
        final BpsRuntimeException exception = new BpsRuntimeException(rootCause);
        assertEquals(rootCause, exception.getCause());

    }
}
