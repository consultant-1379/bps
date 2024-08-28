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
package com.ericsson.component.aia.services.bps.test.pipeExecutor.beans;

import com.ericsson.component.aia.services.bps.test.services.TestBaseContext;

public class TestAdapterBean {

    private String dataFormat;
    private Class<? extends TestBaseContext> adapterType;

    @Override
    public String toString() {
        return "TestAdapterBean [dataFormat=" + dataFormat + ", adapterType=" + adapterType + "]";
    }

    /**
     * @return the dataFormat
     */
    public String getDataFormat() {
        return dataFormat;
    }

    /**
     * @param dataFormat
     *            the dataFormat to set
     */
    public void setDataFormat(final String dataFormat) {
        this.dataFormat = dataFormat;
    }

    /**
     * @return the adapterType
     */
    public Class<? extends TestBaseContext> getAdapterType() {
        return adapterType;
    }

    /**
     * @param adapterType
     *            the adapterType to set
     */
    public void setAdapterType(final Class<? extends TestBaseContext> adapterType) {
        this.adapterType = adapterType;
    }

    /**
     * @param dataFormat
     * @param adapterType
     */
    public TestAdapterBean(final String dataFormat, final Class<? extends TestBaseContext> adapterType) {
        super();
        this.dataFormat = dataFormat;
        this.adapterType = adapterType;
    }

}
