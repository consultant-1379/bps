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
package com.ericsson.component.aia.services.bps.spark.jobrunner.common;

import static com.ericsson.component.aia.model.registry.utils.Constants.SCHEMA_REGISTRY_ADDRESS_PARAMETER;
import static com.ericsson.component.aia.model.registry.utils.Constants.SCHEMA_REGISTRY_CACHE_MAX_SIZE_PARAMETER;
import static com.ericsson.component.aia.services.bps.core.common.Constants.APP_NAME;
import static com.ericsson.component.aia.services.bps.core.common.Constants.MASTER_URL;
import static org.apache.spark.launcher.SparkLauncher.DRIVER_EXTRA_JAVA_OPTIONS;
import static org.apache.spark.launcher.SparkLauncher.EXECUTOR_EXTRA_JAVA_OPTIONS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.NoSuchElementException;
import java.util.Properties;

import org.junit.After;
import org.junit.Test;

public class SparkSessionHelperTest {

    @After
    public void tearDown() throws Exception {
        if (SparkSessionHelper.getSession() != null) {
            SparkSessionHelper.getSession().stop();
        }
    }

    @Test
    public void testInitSparkContext() {
        SparkSessionHelper.initializeSparkContext(null, null, getTestProperties());
        assertNotNull(SparkSessionHelper.getSession());
        final String executorExtraJavaOptions = SparkSessionHelper.getSession().conf().get(EXECUTOR_EXTRA_JAVA_OPTIONS);
        final String driverExtraJavaOptions = SparkSessionHelper.getSession().conf().get(DRIVER_EXTRA_JAVA_OPTIONS);
        assertEquals(executorExtraJavaOptions, " -DschemaRegistry.address=http://localhost:8080 -DschemaRegistry.cacheMaximumSize=20");
        assertEquals(driverExtraJavaOptions, " -DschemaRegistry.address=http://localhost:8080 -DschemaRegistry.cacheMaximumSize=20");
    }

    @Test
    public void testForEmptyConfigParameters() {
        final Properties properties = new Properties();
        properties.setProperty(APP_NAME, "test");
        properties.setProperty(MASTER_URL, "local[*]");
        SparkSessionHelper.initializeSparkContext(null, null, properties);
        final String executorExtraJavaOptions = SparkSessionHelper.getSession().conf().get(EXECUTOR_EXTRA_JAVA_OPTIONS);
        final String driverExtraJavaOptions = SparkSessionHelper.getSession().conf().get(DRIVER_EXTRA_JAVA_OPTIONS);
        assertEquals(executorExtraJavaOptions, driverExtraJavaOptions);
    }

    public void testInitStreamingContext() {
        assertNotNull(SparkSessionHelper.initializeStreamingContext(null, null, getTestProperties()));
    }

    private Properties getTestProperties() {
        final Properties properties = new Properties();
        properties.setProperty(APP_NAME, "test");
        properties.setProperty(MASTER_URL, "local[*]");
        properties.setProperty(SCHEMA_REGISTRY_ADDRESS_PARAMETER, "http://localhost:8080");
        properties.setProperty(SCHEMA_REGISTRY_CACHE_MAX_SIZE_PARAMETER, "20");

        return properties;
    }

    @Test
    public void testForExtraSparkConfParametersSuppliedThroughSteps() {
        final Properties stepProperties = getTestProperties();
        stepProperties.setProperty(APP_NAME, "test");
        stepProperties.setProperty("spark.driver.cores", "2");
        stepProperties.setProperty("spark.eventLog.enabled", "false");
        stepProperties.setProperty("spark.app.name", "sparAppNameThroughCustomAttr");
        SparkSessionHelper.initializeSparkContext(null, null, stepProperties);
        assertNotNull(SparkSessionHelper.getSession());
        final String sparkDriverCores = SparkSessionHelper.getSession().conf().get("spark.driver.cores");
        final String sparkAppName = SparkSessionHelper.getSession().conf().get("spark.app.name");
        final String sparkEventLogEnabled = SparkSessionHelper.getSession().conf().get("spark.eventLog.enabled");
        assertEquals("2", sparkDriverCores);
        assertEquals("sparAppNameThroughCustomAttr", sparkAppName);
        assertEquals("false", sparkEventLogEnabled);
    }

    @Test(expected = NoSuchElementException.class)
    public void testForExtraNonSparkConfParametersSuppliedThroughSteps() {
        final Properties stepProperties = getTestProperties();
        stepProperties.setProperty(APP_NAME, "test");
        stepProperties.setProperty("myCustParam", "2");
        SparkSessionHelper.initializeSparkContext(null, null, stepProperties);
        assertNotNull(SparkSessionHelper.getSession());
        SparkSessionHelper.getSession().conf().get("myCustParam");

    }
}
