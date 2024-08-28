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

import java.util.ArrayList;
import java.util.Collection;

import org.junit.Test;

/**
 * Validate Runtime exception class for bps rule validation exception scenarios
 */
public class BpsRuleValidationExceptionTest {

    /**
     * Test BpsRuleValidationException with specific message.
     */
    @Test
    public void testBpsRuleValidationException() {
        final BpsRuleValidationException exception = new BpsRuleValidationException("BPS-RULE-Validation-Exception");
        assertEquals("BPS-RULE-Validation-Exception", exception.getMessage());
    }

    /**
     * Test BpsRuleValidationException with specific message and root cause exception object.
     */
    @Test
    public void testBpsRuleValidationExceptionWithExceptionObject() {
        final Exception rootCause = new Exception("BPS-RULE-Validation-Exception");
        final BpsRuleValidationException exception = new BpsRuleValidationException("BPS-RULE-Validation-Exception", rootCause);
        assertEquals("BPS-RULE-Validation-Exception", exception.getMessage());
        assertEquals(rootCause, exception.getCause());
    }

    /**
     * Test BpsRuleValidationException with list of messages.
     */
    @Test
    public void testBpsRuleValidationExceptionWithMessages() {
        final Collection<String> messages = new ArrayList<>();
        messages.add("BPS-RULE-Validation 1");
        messages.add("BPS-RULE-Validation 2");
        final BpsRuleValidationException exception = new BpsRuleValidationException(messages);
        assertEquals("[BPS-RULE-Validation 1, BPS-RULE-Validation 2]", exception.getMessage());

    }
}