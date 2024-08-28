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

import com.ericsson.component.aia.services.bps.core.common.uri.IOURIS;
import com.ericsson.component.aia.services.bps.core.service.streams.BpsAbstractDataSink;

/**
 * MockJdbcInputStreamService
 */
@SuppressWarnings("rawtypes")
public class MockJdbcDataSink extends BpsAbstractDataSink {

    @Override
    public void write(final Object dataStream) {
    }

    @Override
    public String getServiceContextName() {
        return IOURIS.JDBC.getUri();
    }

    @Override
    public void cleanUp() {
    }

}
