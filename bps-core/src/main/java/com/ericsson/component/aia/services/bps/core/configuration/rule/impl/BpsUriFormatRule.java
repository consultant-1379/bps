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

import static com.ericsson.component.aia.services.bps.core.common.Rule.URI;

import com.ericsson.component.aia.common.service.GenericService;
import com.ericsson.component.aia.services.bps.core.common.Constants;
import com.ericsson.component.aia.services.bps.core.configuration.rule.BpsRule;
import com.ericsson.component.aia.services.bps.core.configuration.rule.BpsRuleValidationResult;

/**
 * This class implement BpsRule interface for file uri format rule
 */
public class BpsUriFormatRule implements GenericService, BpsRule {

    @Override
    public BpsRuleValidationResult validate(final String value) {
        final BpsRuleValidationResult bpsRuleValidationResult = new BpsRuleValidationResult();
        bpsRuleValidationResult.setSuccessful(value.matches(Constants.REGEX));
        if (!bpsRuleValidationResult.isSuccessful()) {
            bpsRuleValidationResult.setFailureReason(
                    String.format("The value %s supplied for parameter %s does not match the expected format %s", value, URI, Constants.REGEX));
        }
        return bpsRuleValidationResult;
    }

    @Override
    public String getServiceContextName() {
        return URI.getValue();
    }
}
