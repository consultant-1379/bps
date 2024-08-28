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

/**
 * This class holds the validation result and failure reason for each implementation of {@link BpsRule}
 */
public class BpsRuleValidationResult {

    private boolean successful;

    private String failureReason;

    public boolean isSuccessful() {
        return successful;
    }

    public String getFailureReason() {
        return failureReason;
    }

    public void setSuccessful(final boolean successful) {
        this.successful = successful;
    }

    public void setFailureReason(final String failureReason) {
        this.failureReason = failureReason;
    }

}
