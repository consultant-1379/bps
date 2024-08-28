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

import static com.ericsson.component.aia.services.bps.core.common.Rule.FILE_URI;

import org.apache.commons.lang.SystemUtils;

import com.ericsson.component.aia.common.service.GenericService;
import com.ericsson.component.aia.services.bps.core.configuration.rule.BpsRule;
import com.ericsson.component.aia.services.bps.core.configuration.rule.BpsRuleValidationResult;
import com.ericsson.component.aia.services.bps.core.exception.BpsRuntimeException;

/**
 * This class implement BpsRule interface for file uri format rule
 */
public class BpsFileUriFormatRule implements GenericService, BpsRule {

    private static final String REGEX_OS_WINDOWS = "^(.+?:/{3})(.+\\?*)*(.*)*";

    private static final String REGEX_OS_LINUX = "^(.+?:/{2})(.+\\?*)*(.*)*";

    @Override
    public BpsRuleValidationResult validate(final String value) {
        final BpsRuleValidationResult bpsRuleValidationResult = new BpsRuleValidationResult();
        bpsRuleValidationResult.setSuccessful(value.matches(getRegex()));
        if (!bpsRuleValidationResult.isSuccessful()) {
            bpsRuleValidationResult.setFailureReason(
                    String.format("The value %s supplied for parameter %s does not match the expected format %s", value, FILE_URI, getRegex()));
        }
        return bpsRuleValidationResult;
    }

    @Override
    public String getServiceContextName() {
        return FILE_URI.getValue();
    }

    /**
     * This method will return regex used for validation
     *
     * @return the regex used for validation
     */
    public static String getRegex() {
        if (SystemUtils.IS_OS_LINUX || SystemUtils.IS_OS_UNIX) {
            return REGEX_OS_LINUX;
        } else if (SystemUtils.IS_OS_WINDOWS) {
            return REGEX_OS_WINDOWS;
        } else {
            throw new BpsRuntimeException("OS not supported.. Supported OS - LINUX/UNIX/WINDOWS");
        }
    }
}
