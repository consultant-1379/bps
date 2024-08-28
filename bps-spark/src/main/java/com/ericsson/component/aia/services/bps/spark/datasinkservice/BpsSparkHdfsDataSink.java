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

import static com.ericsson.component.aia.services.bps.core.common.uri.IOURIS.HDFS;

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
 * The <code>BpsSparkHdfsDataSink</code> class is responsible for writing {@link Dataset } to a HDFS.<br>
 * The default Partition strategy is {@link SparkDefaultPartition}.
 *
 * @param <C>
 *            the generic type representing the context like HiveContext etc.
 */
@SuppressWarnings("rawtypes")
public class BpsSparkHdfsDataSink<C> extends BpsAbstractDataSink<C, Dataset> {

    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger(BpsSparkHdfsDataSink.class);

    /** The Partition strategy used by the Hdfs Data Sink. */
    protected BpsPartition<Dataset> strategy;

    /**
     * Configured instance of {@link BpsSparkHdfsDataSink} for the specified sinkContextName.<br>
     * sinkContextName is the name of the output sink which was configured in flow.xml through tag &lt;output name="sinkContextName"&gt; ....
     * &lt;/output&gt;
     *
     * @param context
     *            the context for which Sink needs to be configured.
     * @param bpsDataSinkConfiguration
     *            the bps data sink configuration
     */
    @Override
    @SuppressWarnings("CPD-START")
    public void configureDataSink(final C context, final BpsDataSinkConfiguration bpsDataSinkConfiguration) {
        super.configureDataSink(context, bpsDataSinkConfiguration);
        final URIDefinition<IOURIS> decode = IOURIS.decode(properties);
        super.writingContext = HDFS.getUri() + decode.getContext();
        LOGGER.trace("Initiating configureDataSink for {}. ", getDataSinkContextName());
        strategy = new SparkDefaultPartition();
        LOGGER.info("Configuring {} for the output  {} with partition strategy {}", this.getClass().getName(), dataSinkContextName,
                strategy.getClass().getName());
        strategy.init(properties);
        LOGGER.trace("Finished configureDataSink method for the sink context name {} ", dataSinkContextName);
    }

    /**
     * Writes Dataset to HDFS location based on Partition strategy
     */
    @Override
    @SuppressWarnings("CPD-END")
    public void write(final Dataset dataStream) {
        LOGGER.trace("Initiating Write for {}. ", getDataSinkContextName());
        final String path = getWritingContext();
        strategy.write(dataStream, path);
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
        return HDFS.getUri();
    }
}
