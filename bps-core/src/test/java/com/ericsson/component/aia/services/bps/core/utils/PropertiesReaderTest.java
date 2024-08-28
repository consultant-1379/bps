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
package com.ericsson.component.aia.services.bps.core.utils;

import java.util.Properties;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import junit.framework.Assert;

/**
 * Created on 10/26/16.
 */
@RunWith(MockitoJUnitRunner.class)
public class PropertiesReaderTest {

    private Properties properties;

    @Before
    public void setUp() {

        properties = new Properties();

        properties.setProperty("root.logger", "INFO");
        properties.setProperty("log4j.logger.com.ericsson.component", "INFO");

    }

    @Test
    public void testGetProperties() {

        Assert.assertTrue(!PropertiesReader.getProperties("src/test/resources/sparkDefault", properties).isEmpty());

    }

    @Test(expected = IllegalArgumentException.class)
    public void testIllegalArgumentException() {

        PropertiesReader.getProperties("wrong path", properties);
    }

    @Test
    public void testGetPropertyAsString() {

        final String propertyString = PropertiesReader.getPropertyAsString(properties);
        Assert.assertEquals(propertyString, "{(Key:log4j.logger.com.ericsson.component->Value:INFO)(Key:root.logger->Value:INFO)}");

    }
}
