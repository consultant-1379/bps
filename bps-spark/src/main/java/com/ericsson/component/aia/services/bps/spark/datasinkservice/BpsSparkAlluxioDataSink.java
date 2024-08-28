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
package com.ericsson.component.aia.services.bps.spark.datasinkservice;

import static com.ericsson.component.aia.services.bps.core.common.uri.IOURIS.ALLUXIO;

import org.apache.spark.sql.Dataset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.services.bps.core.common.URIDefinition;
import com.ericsson.component.aia.services.bps.core.common.uri.IOURIS;
import com.ericsson.component.aia.services.bps.core.service.configuration.datasink.BpsDataSinkConfiguration;
import com.ericsson.component.aia.services.bps.core.service.partition.BpsPartition;
import com.ericsson.component.aia.services.bps.core.service.streams.BpsAbstractDataSink;
import com.ericsson.component.aia.services.bps.spark.configuration.partition.SparkDefaultPartition;

/**
 * The <code>BpsSparkAlluxioDataSink</code> class is responsible for writing {@link Dataset } to a Alluxio.<br>
 * The default Partition strategy is {@link SparkDefaultPartition}.
 *
 * @param <C>
 *            the generic type representing the context like HiveContext etc.
 */
@SuppressWarnings("rawtypes")
public class BpsSparkAlluxioDataSink<C> extends BpsAbstractDataSink<C, Dataset> {

    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger(BpsSparkAlluxioDataSink.class);

    /** The Partition strategy used by the Alluxio Data Sink. */
    protected BpsPartition<Dataset> strategy;

    /**
     * Configured instance of {@link BpsSparkAlluxioDataSink} for the specified sinkContextName.<br>
     * sinkContextName is the name of the output sink which was configured in flow.xml through tag &lt;output name="sinkContextName"&gt; ....
     * &lt;/output&gt;
     *
     * @param context
     *            the context for which Sink needs to be configured.
     * @param bpsDataSinkConfiguration
     *            the bps data sink configuration
     */
    @Override
    public void configureDataSink(final C context, final BpsDataSinkConfiguration bpsDataSinkConfiguration) {
        super.configureDataSink(context, bpsDataSinkConfiguration);
        LOGGER.trace("Initiating configureDataSink for {}. ", getDataSinkContextName());
        final URIDefinition<IOURIS> decode = IOURIS.decode(properties);
        super.writingContext = ALLUXIO.getUri() + decode.getContext();
        strategy = new SparkDefaultPartition();
        strategy.init(properties);
        LOGGER.trace("Finished configureDataSink method for the sink context name {} ", dataSinkContextName);
    }

    /**
     * Writes Dataset to Alluxio
     */
    @Override
    public void write(final Dataset frame) {
        LOGGER.trace("Initiating Write for {}. ", getDataSinkContextName());
        strategy.write(frame, getWritingContext());
        LOGGER.trace("Finished Write for {}. ", getDataSinkContextName());
    }

    @Override
    public void cleanUp() {
        LOGGER.trace("Cleaning resources allocated for {} ", getDataSinkContextName());
        strategy = null;
        LOGGER.trace("Cleaned resources allocated for {} ", getDataSinkContextName());
    }

    /**
     * Gets the service context name as defined in flow xml.
     *
     * @return the service context name
     */
    @Override
    public String getServiceContextName() {
        return ALLUXIO.getUri();
    }
}
