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

import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.services.bps.core.common.Constants;
import com.ericsson.component.aia.services.bps.core.common.uri.IOURIS;
import com.ericsson.component.aia.services.bps.core.service.configuration.datasink.BpsDataSinkConfiguration;
import com.ericsson.component.aia.services.bps.core.service.streams.BpsAbstractDataSink;
import com.ericsson.component.aia.services.bps.spark.configuration.partition.SparkDefaultPartition;

/**
 * The <code>BpsSparkJdbcDataSink</code> class is responsible for writing {@link Dataset } to a Jdbc.<br>
 * The default Partition strategy is {@link SparkDefaultPartition}.
 *
 * @param <C>
 *            the generic type representing the context like HiveContext etc.
 */
@SuppressWarnings("rawtypes")
public class BpsSparkJdbcDataSink<C> extends BpsAbstractDataSink<C, Dataset> {

    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger(BpsSparkJdbcDataSink.class);

    /** The connection props. */
    private Properties connectionProps;

    /** The table name. */
    private String tableName;

    /** The save mode. */
    private String saveMode;

    /** The p columns. */
    private String[] pColumns;

    /** The is partition enabled. */
    private boolean isPartitionEnabled;

    /**
     * Configured instance of {@link BpsSparkJdbcDataSink} for the specified sinkContextName.<br>
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

        tableName = properties.getProperty(Constants.TABLE);
        saveMode = properties.getProperty("data.save.mode", "Append");
        final String partitionColumes = properties.getProperty(Constants.PARTITION_COLUMNS);
        isPartitionEnabled = ((partitionColumes == null || partitionColumes.trim().length() < 1) ? false : true);

        if (isPartitionEnabled) {
            pColumns = partitionColumes.split(",");
        }

        connectionProps = properties;
        strategy = new SparkDefaultPartition();
        LOGGER.info("Configuring {} for the output  {} with partition strategy {}", this.getClass().getName(), dataSinkContextName,
                strategy.getClass().getName());

        strategy.init(properties);
        LOGGER.trace("Finished configureDataSink method for the sink context name {} ", dataSinkContextName);
    }

    /**
     * Writes Dataset to JDBC data source.
     */
    @Override
    public void write(final Dataset frame) {
        LOGGER.trace("Initiating Write for {}. ", getDataSinkContextName());
        frame.write().mode((saveMode == null || saveMode.trim().length() < 0) ? SaveMode.Append : getMode(saveMode)).partitionBy(pColumns)
                .jdbc(getWritingContext(), tableName, connectionProps);
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
        return IOURIS.JDBC.getUri();
    }

    /**
     * Gets the persisting mode.
     *
     * @param mode
     *            the mode
     * @return the mode
     */
    private SaveMode getMode(final String mode) {

        LOGGER.trace("Entering the getMode method");

        if (mode == null) {
            return SaveMode.Append;
        } else if (("Overwrite").equalsIgnoreCase(mode)) {
            return SaveMode.Overwrite;
        } else if (("Append").equalsIgnoreCase(mode)) {
            return SaveMode.Append;
        } else if (("ErrorIfExists").equalsIgnoreCase(mode)) {
            return SaveMode.ErrorIfExists;
        } else if (("Ignore").equalsIgnoreCase(mode)) {
            return SaveMode.Ignore;
        }

        LOGGER.trace("Exiting the getMode method");

        return SaveMode.Append;
    }
}
