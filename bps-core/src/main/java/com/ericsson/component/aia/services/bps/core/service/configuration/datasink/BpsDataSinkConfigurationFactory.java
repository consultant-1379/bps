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
package com.ericsson.component.aia.services.bps.core.service.configuration.datasink;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.itpf.common.modeling.flow.schema.gen.fbp_flow.AttributeValueType;
import com.ericsson.component.aia.itpf.common.modeling.flow.schema.gen.fbp_flow.OutputType;
import com.ericsson.component.aia.itpf.common.modeling.flow.schema.gen.fbp_flow.SinkType;
import com.ericsson.component.aia.itpf.common.modeling.flow.schema.gen.fbp_flow.SinksType;

/**
 * A factory class for creating Outputstream objects.
 */
public class BpsDataSinkConfigurationFactory {

    /** Logger. */
    private static final Logger LOG = LoggerFactory.getLogger(BpsDataSinkConfigurationFactory.class);

    private BpsDataSinkConfigurationFactory() {

    }

    /**
     * Creates OutputStream based on the configuration.
     *
     * @param outputType
     *            the input output type
     * @return the output stream
     */
    public static BpsDataSinkConfiguration create(final OutputType outputType) {
        LOG.trace("Entering the create method");
        final Properties props = new Properties();
        final String name = outputType.getName();
        final List<AttributeValueType> attribute = outputType.getAttribute();
        props.put("output.name", name);

        for (final AttributeValueType attributeValueType : attribute) {
            props.put(attributeValueType.getName(), attributeValueType.getValue());
        }

        final DefaultBpsDataSinkConfiguration uriOutput = new DefaultBpsDataSinkConfiguration();

        final SinksType sinksType = outputType.getSinks();
        if (sinksType != null) {
            uriOutput.configure(name, props, sinksType.getSink());
        } else {
            uriOutput.configure(name, props, Collections.<SinkType> emptyList());
        }

        LOG.trace("Exiting the create method");
        return uriOutput;
    }

}
