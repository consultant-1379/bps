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
package com.ericsson.component.aia.services.bps.core.configuration.rule;

import com.ericsson.component.aia.common.service.GenericService;

/**
 * This interface represents the Bps Rule. All implementation should implement this interface for provide provider specific rule validation logic
 */
public interface BpsRule extends GenericService {

    /**
     * This method will validate the value provide as parameter
     *
     * @param value
     *            to validate
     * @return {@link BpsRuleValidationResult} containing validation result
     */
    BpsRuleValidationResult validate(final String value);
}
