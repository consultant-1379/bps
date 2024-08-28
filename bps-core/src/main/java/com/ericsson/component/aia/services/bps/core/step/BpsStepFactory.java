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

import java.io.Serializable;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.common.service.loader.GenericServiceLoader;
import com.ericsson.component.aia.itpf.common.modeling.flow.schema.gen.fbp_flow.AttributeValueType;
import com.ericsson.component.aia.itpf.common.modeling.flow.schema.gen.fbp_flow.StepType;
import com.ericsson.component.aia.services.bps.core.common.Constants;
import com.ericsson.component.aia.services.bps.core.common.uri.PROCESS_URIS;
import com.ericsson.component.aia.services.bps.core.service.BpsJobRunner;

/**
 * A factory for creating Step objects.
 */
public class BpsStepFactory implements Serializable {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 2854654812257328592L;

    /** Logger. */
    private static final Logger LOG = LoggerFactory.getLogger(BpsStepFactory.class);

    private BpsStepFactory() {

    }

    /**
     * Creates the Step based on the configuration.
     *
     * @param stepType
     *            the step type
     * @return the step
     */
    public static BpsStep create(final StepType stepType) {
        LOG.trace("Entering the create method");
        final Properties props = new Properties();
        final String name = stepType.getName();
        final List<AttributeValueType> attribute = stepType.getAttribute();
        props.put("step.name", name);

        for (final AttributeValueType attributeValueType : attribute) {
            props.put(attributeValueType.getName(), attributeValueType.getValue());
        }

        final BpsDefaultStep defaultStep = new BpsDefaultStep();
        defaultStep.configure(name, props);
        defaultStep.setStepHandler(getStepHandler(props));
        LOG.trace("Exiting the create method");
        return defaultStep;
    }

    /**
     * Gets the step handler.
     *
     * @param props
     *            the props
     * @return the step handler
     */
    private static BpsJobRunner getStepHandler(final Properties props) {
        final String uri = props.getProperty(Constants.URI);
        final PROCESS_URIS processUri = PROCESS_URIS.findUri(uri);
        return (BpsJobRunner) GenericServiceLoader.getService(BpsJobRunner.class, processUri.getUri());
    }

}
