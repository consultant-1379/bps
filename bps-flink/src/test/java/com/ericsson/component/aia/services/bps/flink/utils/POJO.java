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
package com.ericsson.component.aia.services.bps.flink.utils;

import java.io.Serializable;

/**
 * Sample pojo class
 */
@SuppressWarnings("PMD")
public class POJO implements Serializable {

    private long SCANNER_ID;

    private long RBS_MODULE_ID;

    public long getSCANNER_ID() {
        return SCANNER_ID;
    }

    public void setSCANNER_ID(final long sCANNER_ID) {
        SCANNER_ID = sCANNER_ID;
    }

    public long getRBS_MODULE_ID() {
        return RBS_MODULE_ID;
    }

    public void setRBS_MODULE_ID(final long rBS_MODULE_ID) {
        RBS_MODULE_ID = rBS_MODULE_ID;
    }

    @Override
    public String toString() {
        final StringBuilder stringBuilder = new StringBuilder("POJO [ SCANNER_ID=");
        stringBuilder.append(SCANNER_ID);
        stringBuilder.append(" ,RBS_MODULE_ID=");
        stringBuilder.append(RBS_MODULE_ID).append(" ]");
        return stringBuilder.toString();
    }
}
