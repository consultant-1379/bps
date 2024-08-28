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
package com.ericsson.component.aia.services.bps.core.service;

import java.util.Properties;

import com.ericsson.component.aia.services.bps.core.common.uri.PROCESS_URIS;
import com.ericsson.component.aia.services.bps.core.service.configuration.datasink.BpsDataSinkAdapters;
import com.ericsson.component.aia.services.bps.core.service.configuration.datasource.BpsDataSourceAdapters;

public class MockSparkJobRunner implements BpsJobRunner {

    private static final long serialVersionUID = 1L;

    @Override
    public String getServiceContextName() {
        return PROCESS_URIS.SPARK_BATCH.getUri();
    }

    @Override
    public void execute() {
    }

    @Override
    public void initialize(final BpsDataSourceAdapters inputAdapters, final BpsDataSinkAdapters outputAdapters, final Properties properties) {
    }

    @Override
    public void cleanUp() {
    }

    @Override
    public void executeJob() {
        // TODO Auto-generated method stub

    }

}
