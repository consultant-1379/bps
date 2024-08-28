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
package com.ericsson.component.aia.services.bps.core.service.configuration.datasink;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * Created on 10/24/16.
 */
@RunWith(MockitoJUnitRunner.class)
public class BpsDataSinkAdaptersTest {

    private BpsDataSinkAdapters bpsDataSinkAdapters = new BpsDataSinkAdapters();

    @Mock
    private BpsDataSinkConfiguration bpsDataSinkConfiguration;

    private final List<BpsDataSinkConfiguration> bpsDataSinkConfigurations = new ArrayList<>();

    @Before
    public void setUp() {

        bpsDataSinkConfigurations.add(bpsDataSinkConfiguration);

    }

    @Test
    public void testGetBpsDataSinkConfigurations() {

        assertTrue(bpsDataSinkAdapters.getBpsDataSinkConfigurations().isEmpty());

    }

    @Test
    public void testAddBpsDataSinkConfiguration() {

        Mockito.when(bpsDataSinkConfiguration.getDataSinkContextName()).thenReturn("BpsDataSink");
        bpsDataSinkAdapters.addBpsDataSinkConfiguration(bpsDataSinkConfiguration);
        assertTrue(bpsDataSinkConfiguration.getDataSinkContextName().equals("BpsDataSink"));
    }

    @Test
    public void testToString() {
        assertTrue(bpsDataSinkAdapters.toString()
                .equals("BpsDataSinkAdapters [BpsDataSinkConfigurations=" + bpsDataSinkAdapters.getBpsDataSinkConfigurations() + "]"));

    }
}
