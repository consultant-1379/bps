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
package com.ericsson.component.aia.services.bps.core.service.configuration.datasource;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * Created on 10/24/16.
 */
@RunWith(MockitoJUnitRunner.class)
public class BpsDataSourceAdaptersTest {

    @Spy
    private BpsDataSourceAdapters bpsDataSourceAdapters;

    private final List<BpsDataSourceConfiguration> bpsDataSourceConfigurations = new ArrayList<>();

    @Mock
    private BpsDataSourceConfiguration bpsDataSourceConfiguration;

    @Test
    public void testGetBpsDataSourceConfiguration() {

        bpsDataSourceConfigurations.add(bpsDataSourceConfiguration);
        Mockito.when(bpsDataSourceAdapters.getBpsDataSourceConfigurations()).thenReturn(bpsDataSourceConfigurations);
        assertTrue(!bpsDataSourceAdapters.getBpsDataSourceConfigurations().isEmpty());
    }

    @Test
    public void testAddBpsDataSourceConfiguration() {

        bpsDataSourceAdapters.addBpsDataSourceConfiguration(bpsDataSourceConfiguration);
        Mockito.when(bpsDataSourceConfiguration.getDataSourceContextName()).thenReturn("BpsDataSource");
        assertTrue(bpsDataSourceConfiguration.getDataSourceContextName().equals("BpsDataSource"));

    }

    @Test
    public void testToString() {

        bpsDataSourceAdapters.addBpsDataSourceConfiguration(bpsDataSourceConfiguration);

        assertTrue(bpsDataSourceAdapters.toString()
                .equals("InputStreamAdapters " + "[inputStreams=" + bpsDataSourceAdapters.getBpsDataSourceConfigurations() + "]"));

    }

}
