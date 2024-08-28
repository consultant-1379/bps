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
package com.ericsson.component.aia.services.bps.core.step;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.services.bps.core.common.Constants;
import com.ericsson.component.aia.services.bps.core.common.uri.PROCESS_URIS;
import com.ericsson.component.aia.services.bps.core.service.BpsJobRunner;

/**
 * DefaultStep class holds/refer different Steps.
 */
public class BpsDefaultStep implements BpsStep {

    /** Logger. */
    private static final Logger LOG = LoggerFactory.getLogger(BpsDefaultStep.class);

    /** The properties. */
    private Properties properties;

    /** The context. */
    private String context;

    /** The step handler. */
    private BpsJobRunner stepHandler;

    /**
     * configure method initializes different Step after mandatory fields check for a pipe.
     *
     * @param contextName
     *            the context name
     * @param props
     *            the props
     * @return true, if successful
     */
    @Override
    public boolean configure(final String contextName, final Properties props) {
        LOG.trace("Entering the configure method");
        this.properties = props;
        this.context = contextName;
        LOG.trace("Exiting the configure method");
        return true;
    }

    /**
     * Execute method run a Step.
     */
    @Override
    public void execute() {
        LOG.trace("Entering the execute method");
        if (null == stepHandler) {
            throw new IllegalArgumentException("Missing Step handler for " + context);
        }
        stepHandler.execute();
        LOG.trace("Exiting the execute method");
    }

    /**
     * This operation will do the clean up for Step.
     */
    @Override
    public void cleanUp() {
        LOG.trace("Entering the cleanUp method");
        if (null == stepHandler) {
            throw new IllegalArgumentException("Missing Step handler for " + context);
        }
        stepHandler.cleanUp();
        LOG.trace("Exiting the cleanUp method");
    }

    @Override
    public Properties getconfiguration() {
        return properties;
    }

    @Override
    public String getName() {
        return context;
    }

    /**
     * Gets the properties.
     *
     * @return the properties
     */
    public Properties getProperties() {
        return properties;
    }

    /**
     * Sets the properties.
     *
     * @param properties
     *            the new properties
     */
    public void setProperties(final Properties properties) {
        this.properties = properties;
    }

    @Override
    public String toString() {
        final String uri = properties.getProperty(Constants.URI);
        final PROCESS_URIS processUri = PROCESS_URIS.findUri(uri);
        return "[StepName=" + processUri.getUri() + " contextName=" + context + "]";
    }

    @Override
    public BpsJobRunner getStepHandler() {
        return stepHandler;
    }

    @Override
    public void setStepHandler(final BpsJobRunner stepHandler) {
        this.stepHandler = stepHandler;
    }
}
