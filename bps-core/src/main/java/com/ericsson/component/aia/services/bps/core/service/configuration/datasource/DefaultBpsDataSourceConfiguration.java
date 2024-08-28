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

import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.itpf.common.modeling.flow.schema.gen.fbp_flow.SourceType;
import com.ericsson.component.aia.services.bps.core.common.Constants;
import com.ericsson.component.aia.services.bps.core.common.uri.IOURIS;

/**
 * DefaultInputs class holds/refer different bps data source.
 */
public class DefaultBpsDataSourceConfiguration implements BpsDataSourceConfiguration {

    /** Logger. */
    private static final Logger LOG = LoggerFactory.getLogger(DefaultBpsDataSourceConfiguration.class);

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 6691622855599610509L;

    /** The context name. */
    private String dataSourceContextName;

    /** The properties. */
    private Properties properties;

    private List<SourceType> sources;

    /**
     * configure method initialises different BPS data sources after mandatory fields check for a stream.
     *
     * @param dataSourceContextName
     *            The context name.
     * @param props
     *            The props.
     */
    @Override
    public void configure(final String dataSourceContextName, final Properties props, final List<SourceType> sources) {
        LOG.trace("Entering the configure method");
        properties = props;
        this.dataSourceContextName = dataSourceContextName;
        this.sources = sources;
        LOG.trace("Exiting the configure method");
    }

    @Override
    public String getDataSourceContextName() {
        return dataSourceContextName;
    }

    @Override
    public Properties getDataSourceConfiguration() {
        return properties;
    }

    @Override
    public String toString() {
        final String uri = properties.getProperty(Constants.URI);
        return "[InputName=" + IOURIS.findUri(uri) + " contextName=" + dataSourceContextName + "]";
    }

    @Override
    public List<SourceType> getSources() {
        return sources;
    }
}
