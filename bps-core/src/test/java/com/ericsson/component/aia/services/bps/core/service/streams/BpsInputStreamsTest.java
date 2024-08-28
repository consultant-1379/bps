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
package com.ericsson.component.aia.services.bps.core.service.streams;

import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * Created on 10/21/16.
 */

@RunWith(MockitoJUnitRunner.class)
public class BpsInputStreamsTest {

    private final BpsInputStreams bpsInputStreams = new BpsInputStreams();

    @Mock
    private BpsBaseStream bpsBaseStream;

    @Before
    public void setUp() {

        bpsInputStreams.add("dataSourceContextName", bpsBaseStream);

    }

    @Test
    public void testGetStreams() {

        assertTrue(bpsInputStreams.getStreams("dataSourceContextName") != null);
    }

    @Test(expected = IllegalStateException.class)
    public void testIllegalStateException() {
        bpsInputStreams.getStreams("");

    }

}
