/**
 *
 * (C) Copyright LM Ericsson System Expertise AT/LMI, 2016
 *
 * The copyright to the computer program(s) herein is the property of Ericsson  System Expertise EEI, Sweden.
 * The program(s) may be used and/or copied only with the written permission from Ericsson System Expertise
 * AT/LMI or in  * accordance with the terms and conditions stipulated in the agreement/contract under which
 * the program(s) have been supplied.
 *
 */
package com.ericsson.component.aia.services.bps.core.utils;

import java.util.Enumeration;
import java.util.Properties;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PropertiesReader is a utility class for all properties related operations.
 */
public class PropertiesReader {

    /** Logger. */
    private static final Logger LOG = LoggerFactory.getLogger(PropertiesReader.class);

    private PropertiesReader() {

    }

    /**
     * Gets the properties.
     *
     * @param defaultPropsPath
     *            the default props path
     * @param customProps
     *            the custom props
     * @return the properties
     */
    public static CompositeConfiguration getProperties(final String defaultPropsPath, final Properties customProps) {

        final CompositeConfiguration config = new CompositeConfiguration();
        try {
            config.addConfiguration(new PropertiesConfiguration(defaultPropsPath + ".properties"));
        } catch (final ConfigurationException exp) {
            LOG.error("Exception while processing getProperties(String defaultPropsPath, Properties customProps): " + exp.getMessage());
            throw new IllegalArgumentException(exp);
        }

        for (final String key : customProps.stringPropertyNames()) {
            final String value = customProps.getProperty(key);
            config.setProperty(key, value);
        }

        return config;
    }

    /**
     * Gets the property as string.
     *
     * @param props
     *            the props
     * @return the property as string
     */
    public static String getPropertyAsString(final Properties props) {

        final StringBuilder sbd = new StringBuilder("{");
        final Enumeration<?> enumeration = props.propertyNames();

        while (enumeration.hasMoreElements()) {

            final String key = (String) enumeration.nextElement();
            final String value = props.getProperty(key).trim();

            sbd.append("(Key:" + key + "->Value:" + value + ")");
        }

        sbd.append("}");

        return sbd.toString();
    }
}