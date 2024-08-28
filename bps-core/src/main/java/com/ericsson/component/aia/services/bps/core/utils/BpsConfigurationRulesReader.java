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

import java.util.Properties;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.services.bps.core.exception.BpsRuntimeException;

/**
 * PropertiesReader is a utility class for all properties related operations.
 */
public class BpsConfigurationRulesReader {

    /** Logger. */
    private static final Logger LOG = LoggerFactory.getLogger(BpsConfigurationRulesReader.class);

    private static final String INGRESS_MANDATORY_PREFIX = "ingress.mandatory";

    private static final String INGRESS_OPTIONAL_PREFIX = "ingress.optional";

    private static final String EGRESS_MANDATORY_PREFIX = "egress.mandatory";

    private static final String EGRESS_OPTIONAL_PREFIX = "egress.optional";

    private static final String STEP_MANDATORY_PREFIX = "step.mandatory";

    private static final String STEP_OPTIONAL_PREFIX = "step.optional";

    private static final String DOT_DELIMITER = ".";

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
        LOG.trace("Entering the method setProperties(String filename, boolean isStep)");
        try {
            compositeConfiguration = new CompositeConfiguration();
            compositeConfiguration.addConfiguration(new PropertiesConfiguration("io.properties"));
            compositeConfiguration.addConfiguration(new PropertiesConfiguration("step.properties"));
        } catch (final ConfigurationException ex) {
            LOG.error("Exception while processing setProperties(String filename, boolean isStep): " + ex.getMessage());
            throw new BpsRuntimeException(ex);
        }
    }

    private BpsConfigurationRulesReader() {

    }

    /**
     * This method will return {@link Properties} containing all the mandatory ingress rules configured for the context provided as parameter
     *
     * @param context
     *            of the mandatory ingress rules
     * @return {@link Properties} containing all the mandatory ingress rules configured for the context provided as parameter
     */
    public static Properties getIngressMandatoryProperties(final String context) {
        return getProperties(INGRESS_MANDATORY_PREFIX + DOT_DELIMITER + context);
    }

    /**
     * This method will return {@link Properties} containing all the optional ingress rules configured for the context provided as parameter
     *
     * @param context
     *            of the optional ingress rules
     * @return {@link Properties} containing all the optional ingress rules configured for the context provided as parameter
     */
    public static Properties getIngressOptionalProperties(final String context) {
        return getProperties(INGRESS_OPTIONAL_PREFIX + DOT_DELIMITER + context);
    }

    /**
     * This method will return {@link Properties} containing all the mandatory egress rules configured for the context provided as parameter
     *
     * @param context
     *            of the mandatory egress rules
     * @return {@link Properties} containing all the mandatory egress rules configured for the context provided as parameter
     */
    public static Properties getEgressMandatoryProperties(final String context) {
        return getProperties(EGRESS_MANDATORY_PREFIX + DOT_DELIMITER + context);
    }

    /**
     * This method will return {@link Properties} containing all the optional egress rules configured for the context provided as parameter
     *
     * @param context
     *            of the optional egress rules
     * @return {@link Properties} containing all the optional egress rules configured for the context provided as parameter
     */
    public static Properties getEgressOptionalProperties(final String context) {
        return getProperties(EGRESS_OPTIONAL_PREFIX + DOT_DELIMITER + context);
    }

    /**
     * This method will return {@link Properties} containing all the mandatory step rules configured for the context provided as parameter
     *
     * @param context
     *            of the mandatory step rules
     * @return {@link Properties} containing all the mandatory step rules configured for the context provided as parameter
     */
    public static Properties getStepMandatoryProperties(final String context) {
        return getProperties(STEP_MANDATORY_PREFIX + DOT_DELIMITER + context);
    }

    /**
     * This method will return {@link Properties} containing all the optional step rules configured for the context provided as parameter
     *
     * @param context
     *            of the optional step rules
     * @return {@link Properties} containing all the optional step rules configured for the context provided as parameter
     */
    public static Properties getStepOptionalProperties(final String context) {
        return getProperties(STEP_OPTIONAL_PREFIX + DOT_DELIMITER + context);
    }

    private static Properties getProperties(final String prefix) {
        final Configuration subset = compositeConfiguration.subset(prefix);
        return ConfigurationConverter.getProperties(subset);
    }
}