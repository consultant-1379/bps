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

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.Properties;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.ericsson.component.aia.common.service.loader.GenericServiceLoader;
import com.ericsson.component.aia.itpf.common.modeling.flow.schema.gen.fbp_flow.SinkType;
import com.ericsson.component.aia.services.bps.core.common.uri.IOURIS;
import com.ericsson.component.aia.services.bps.core.service.BpsDataSinkService;
import com.ericsson.component.aia.services.bps.core.service.MockJdbcDataSink;
import com.ericsson.component.aia.services.bps.core.service.configuration.datasink.DefaultBpsDataSinkConfiguration;

@RunWith(MockitoJUnitRunner.class)
public class BpsOutputSinksTest {

    static BpsOutputSinks outputSinks = new BpsOutputSinks();

    @BeforeClass
    public static void setup() {
        final BpsDataSinkService dataSinkService = (BpsDataSinkService) GenericServiceLoader.getService(BpsDataSinkService.class,
                IOURIS.JDBC.getUri());
        final Properties properties = new Properties();
        properties.put("uri", "JDBC://jdbc:postgresql://localhost:5432/postgres");

        final DefaultBpsDataSinkConfiguration bpsDataSinkConfiguration = new DefaultBpsDataSinkConfiguration();
        bpsDataSinkConfiguration.configure("jdbc-input", properties, Collections.<SinkType> emptyList());

        dataSinkService.configureDataSink("jdbc-input", bpsDataSinkConfiguration);
        outputSinks.addBpsDataSinkService(dataSinkService);
    }

    @Test(expected = IllegalStateException.class)
    public void testGetBpsDataSinkServiceByInvalidContextName() {
        outputSinks.getBpsDataSinkService("jdbc-XYZ");
    }

    @Test
    public void testGetBpsDataSinkServiceByValidContextName() {
        final BpsDataSinkService<?, ?> bpsDataSinkService = outputSinks.getBpsDataSinkService("jdbc-input");
        assertEquals(MockJdbcDataSink.class, bpsDataSinkService.getClass());
    }

    @Test
    public void testCleanUpCall() {
        outputSinks.cleanUp();
        Assert.assertTrue(true);
    }

    @Test
    public void testWriteMethodCall() {
        outputSinks.write(null);
        Assert.assertTrue(true);
    }

}