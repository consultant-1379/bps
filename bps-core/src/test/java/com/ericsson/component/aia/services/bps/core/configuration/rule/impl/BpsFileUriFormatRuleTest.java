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
package com.ericsson.component.aia.services.bps.core.configuration.rule.impl;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.commons.lang.SystemUtils;
import org.junit.Test;

import com.ericsson.component.aia.services.bps.core.exception.BpsRuntimeException;

/**
 * JUNIT class to test BpsFileUriFormatRuleTest
 */
public class BpsFileUriFormatRuleTest {

    private BpsFileUriFormatRule bpsFileUriFormatRule = new BpsFileUriFormatRule();

    @Test
    public void shouldSucceedWhenValidFileUriIsProvided() {
        assertTrue(bpsFileUriFormatRule.validate(getValidUriForOS()).isSuccessful());
    }

    @Test
    public void shouldFailWhenEmptyUriIsProvided() {
        assertFalse(bpsFileUriFormatRule.validate("").isSuccessful());
    }

    @Test
    public void shouldFailWhenInvalidUriIsProvided() {
        assertFalse(bpsFileUriFormatRule.validate(getInValidUriForOS()).isSuccessful());
    }

    public static String getValidUriForOS() {
        if (SystemUtils.IS_OS_LINUX || SystemUtils.IS_OS_UNIX) {
            return "file://C:\\Users";
        } else if (SystemUtils.IS_OS_WINDOWS) {
            return "file:///C:\\Users";
        } else {
            throw new BpsRuntimeException("OS not supported.. Supported OS - LINUX/UNIX/WINDOWS");
        }
    }

    public static String getInValidUriForOS() {
        if (SystemUtils.IS_OS_LINUX || SystemUtils.IS_OS_UNIX) {
            return "file:/C:\\Users";
        } else if (SystemUtils.IS_OS_WINDOWS) {
            return "file://C:\\Users";
        } else {
            throw new BpsRuntimeException("OS not supported.. Supported OS - LINUX/UNIX/WINDOWS");
        }
    }
}
