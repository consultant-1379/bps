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
package com.ericsson.component.aia.services.bps.core.service.configuration.datasource;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.itpf.common.modeling.flow.schema.gen.fbp_flow.AttributeValueType;
import com.ericsson.component.aia.itpf.common.modeling.flow.schema.gen.fbp_flow.InputType;
import com.ericsson.component.aia.itpf.common.modeling.flow.schema.gen.fbp_flow.SourceType;
import com.ericsson.component.aia.itpf.common.modeling.flow.schema.gen.fbp_flow.SourcesType;

/**
 * A factory class for creating Bps Data Source configuration objects.
 */
public class BpsDataSourceConfigurationFactory {

    /** Logger. */
    private static final Logger LOG = LoggerFactory.getLogger(BpsDataSourceConfigurationFactory.class);

    private BpsDataSourceConfigurationFactory() {

    }

    /**
     * Creates BPS data source configuration.
     *
     * @param inputType
     *            the input output type
     * @return the input stream
     */
    public static BpsDataSourceConfiguration create(final InputType inputType) {
        LOG.trace("Entering the create method");
        final Properties props = new Properties();
        final String name = inputType.getName();
        final List<AttributeValueType> attribute = inputType.getAttribute();
        props.put("input.name", name);

        for (final AttributeValueType attributeValueType : attribute) {
            props.put(attributeValueType.getName(), attributeValueType.getValue());
        }

        final DefaultBpsDataSourceConfiguration uriInput = new DefaultBpsDataSourceConfiguration();

        final SourcesType sourcesType = inputType.getSources();
        if (sourcesType != null) {
            uriInput.configure(name, props, sourcesType.getSource());
        } else {
            uriInput.configure(name, props, Collections.<SourceType> emptyList());
        }

        LOG.trace("Exiting the create method");
        return uriInput;
    }
}
