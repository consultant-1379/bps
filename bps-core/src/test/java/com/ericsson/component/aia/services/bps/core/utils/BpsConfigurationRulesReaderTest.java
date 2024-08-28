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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.ericsson.component.aia.services.bps.core.exception.BpsRuntimeException;

/**
 * Created on 10/26/16.
 */
@RunWith(MockitoJUnitRunner.class)
public class BpsConfigurationRulesReaderTest {

    /** The props. */
    private static CompositeConfiguration compositeConfiguration;

    /**
     * Sets the properties.
     *
     * @param filename
     *            the filename
     * @param isStep
     *            the is step
     */
    static {

        try {
            compositeConfiguration = new CompositeConfiguration();
            compositeConfiguration.addConfiguration(new PropertiesConfiguration("src/test/resources/io.properties"));
            compositeConfiguration.addConfiguration(new PropertiesConfiguration("src/test/resources/step.properties"));
        } catch (final ConfigurationException ex) {
            throw new BpsRuntimeException(ex);
        }
    }

    @Test
    public void testGetIngressMandatoryProperties() {

        final List<String> ExpectedPropertiesList = new ArrayList<String>();
        ExpectedPropertiesList.add("{=uri-rule}");
        ExpectedPropertiesList.add("{=attribute-rule}");
        ExpectedPropertiesList.add("{=attribute-rule}");
        ExpectedPropertiesList.add("{=attribute-rule}");
        ExpectedPropertiesList.add("{=attribute-rule}");

        final List ingressMandatoryIOProperties = new ArrayList();
        ingressMandatoryIOProperties.add("jdbc.uri");
        ingressMandatoryIOProperties.add("jdbc.table.name");
        ingressMandatoryIOProperties.add("jdbc.driver");
        ingressMandatoryIOProperties.add("jdbc.user");
        ingressMandatoryIOProperties.add("jdbc.password");

        for (int i = 0; i < ingressMandatoryIOProperties.size(); i++) {
            Properties properties = BpsConfigurationRulesReader.getIngressMandatoryProperties(ingressMandatoryIOProperties.get(i).toString());
            Assert.assertEquals("Properties returned:", ExpectedPropertiesList.get(i).toString(), properties.toString());
        }

    }

    @Test
    public void testGetEngressMandatoryProperties() {

        final List<String> ExpectedPropertiesList = new ArrayList<String>();
        ExpectedPropertiesList.add("{=uri-rule}");
        ExpectedPropertiesList.add("{=attribute-rule}");
        ExpectedPropertiesList.add("{=attribute-rule}");
        ExpectedPropertiesList.add("{=attribute-rule}");
        ExpectedPropertiesList.add("{=attribute-rule}");
        ExpectedPropertiesList.add("{=uri-rule}");
        ExpectedPropertiesList.add("{=attribute-rule}");
        ExpectedPropertiesList.add("{=attribute-rule}");

        final List engressMandatoryIOProperties = new ArrayList();
        engressMandatoryIOProperties.add("jdbc.uri");
        engressMandatoryIOProperties.add("jdbc.table.name");
        engressMandatoryIOProperties.add("jdbc.driver");
        engressMandatoryIOProperties.add("jdbc.user");
        engressMandatoryIOProperties.add("jdbc.password");
        engressMandatoryIOProperties.add("kafka.uri");
        engressMandatoryIOProperties.add("kafka.bootstrap.servers");
        engressMandatoryIOProperties.add("kafka.eventType");

        for (int i = 0; i < engressMandatoryIOProperties.size(); i++) {
            Properties properties = BpsConfigurationRulesReader.getEgressMandatoryProperties(engressMandatoryIOProperties.get(i).toString());
            Assert.assertEquals("Properties returned:", ExpectedPropertiesList.get(i).toString(), properties.toString());
        }

    }

    @Test
    public void testGetStepMandatoryProperties() {

        final List<String> ExpectedPropertiesList = new ArrayList<String>();
        ExpectedPropertiesList.add("{=uri-rule}");
        ExpectedPropertiesList.add("{=attribute-rule}");
        ExpectedPropertiesList.add("{=uri-rule}");

        final List stepMandatoryProperties = new ArrayList();
        stepMandatoryProperties.add("spark-batch.uri");
        stepMandatoryProperties.add("spark-streaming.driver-class");
        stepMandatoryProperties.add("spark-streaming.uri");

        for (int i = 0; i < stepMandatoryProperties.size(); i++) {
            Properties properties = BpsConfigurationRulesReader.getStepMandatoryProperties(stepMandatoryProperties.get(i).toString());
            Assert.assertEquals("Properties returned:", ExpectedPropertiesList.get(i).toString(), properties.toString());
        }

    }

    @Test
    public void testEngressOptionalProperties() {

        final List<String> ExpectedPropertiesList = new ArrayList<String>();
        ExpectedPropertiesList.add("{=attribute-rule}");
        ExpectedPropertiesList.add("{=attribute-rule}");

        final List engressMandatoryIOProperties = new ArrayList();

        engressMandatoryIOProperties.add("kafka.schemaRegistry.address");
        engressMandatoryIOProperties.add("kafka.schemaRegistry.cacheMaximumSize");

        for (int i = 0; i < engressMandatoryIOProperties.size(); i++) {
            Properties properties = BpsConfigurationRulesReader.getEgressOptionalProperties(engressMandatoryIOProperties.get(i).toString());
            Assert.assertEquals("Properties returned:", ExpectedPropertiesList.get(i).toString(), properties.toString());
        }
    }

    @Test
    public void testEngressmandatorypropertiesNotFound() {

        final List<String> ExpectedPropertiesList = new ArrayList<String>();
        ExpectedPropertiesList.add("{}");
        ExpectedPropertiesList.add("{=attribute-rule}");
        ExpectedPropertiesList.add("{}");

        final List engressMandatoryIOProperties = new ArrayList();
        engressMandatoryIOProperties.add("jdbc.uri.wrong");
        engressMandatoryIOProperties.add("jdbc.table.name");
        engressMandatoryIOProperties.add("jdbc.driver.wrong");

        for (int i = 0; i < engressMandatoryIOProperties.size(); i++) {
            Properties properties = BpsConfigurationRulesReader.getEgressMandatoryProperties(engressMandatoryIOProperties.get(i).toString());
            Assert.assertEquals("Properties returned:", ExpectedPropertiesList.get(i).toString(), properties.toString());
        }

    }

    @Test(expected = ConfigurationException.class)
    public void testConfigurationException() throws Exception {

        compositeConfiguration.addConfiguration(new PropertiesConfiguration("/src/test/resources/io.properties"));

    }
}
