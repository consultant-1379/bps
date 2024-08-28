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

import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.itpf.common.modeling.flow.schema.gen.fbp_flow.SinkType;
import com.ericsson.component.aia.services.bps.core.common.Constants;
import com.ericsson.component.aia.services.bps.core.common.uri.IOURIS;

/**
 * DefaultOutputs class holds/refer different bps data source sinks.
 */
public class DefaultBpsDataSinkConfiguration implements BpsDataSinkConfiguration {

    /** Logger. */
    private static final Logger LOG = LoggerFactory.getLogger(DefaultBpsDataSinkConfiguration.class);

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 1L;

    /** The context name. */
    private String dataSinkContextName;

    /** The properties. */
    private Properties properties;

    private List<SinkType> sinks;

    /**
     * configure method initialises different OutputStreams after mandatory fields check for a stream.
     *
     * @param dataSinkContextName
     *            the data sink context name
     * @param props
     *            the props
     * @param sinks
     *            the sinks
     */
    @Override
    public void configure(final String dataSinkContextName, final Properties props, final List<SinkType> sinks) {
        LOG.trace("Entering the configure method");
        properties = props;
        this.dataSinkContextName = dataSinkContextName;
        this.sinks = sinks;
        LOG.trace("Exiting the configure method");
    }

    @Override
    public String getDataSinkContextName() {
        return dataSinkContextName;
    }

    @Override
    public Properties getDataSinkConfiguration() {
        return properties;
    }

    @Override
    public List<SinkType> getSinks() {
        return sinks;
    }

    @Override
    public String toString() {
        final String uri = properties.getProperty(Constants.URI);
        return "[OutputName=" + IOURIS.findUri(uri) + " contextName=" + dataSinkContextName + "]";
    }
}
