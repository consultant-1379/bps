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

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.services.bps.core.common.Constants;
import com.ericsson.component.aia.services.bps.core.common.uri.IOURIS;
import com.ericsson.component.aia.services.bps.core.service.configuration.datasink.BpsDataSinkConfiguration;
import com.ericsson.component.aia.services.bps.core.service.streams.BpsAbstractDataSink;
import com.ericsson.component.aia.services.bps.spark.configuration.partition.SparkDefaultPartition;
import com.ericsson.component.aia.services.bps.spark.utils.SparkUtil;
import com.ericsson.component.aia.services.bps.spark.utils.TransformerUtils;

/**
 * The <code>BpsSparkHiveDataSink</code> class is responsible for writing {@link Dataset } to a Hive.<br>
 * The default Partition strategy is {@link SparkDefaultPartition}.
 *
 * @param <C>
 *            the generic type representing the context like HiveContext etc.
 */
@SuppressWarnings("rawtypes")
public class BpsSparkHiveDataSink<C> extends BpsAbstractDataSink<C, Dataset> {

    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger(BpsSparkHiveDataSink.class);

    /** The format. */
    private String format;

    /** The is partition enabled. */
    private boolean isPartitionEnabled;

    /** The p columns. */
    private String[] pColumns;

    /**
     * Configured instance of {@link BpsSparkHiveDataSink} for the specified sinkContextName.<br>
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
        final String partitionColumes = properties.getProperty(Constants.PARTITION_COLUMNS);
        format = SparkUtil.getFormat(properties);
        isPartitionEnabled = ((partitionColumes == null || partitionColumes.trim().length() < 1) ? false : true);

        if (isPartitionEnabled) {
            pColumns = partitionColumes.split(",");
        }
        strategy = new SparkDefaultPartition();
        LOGGER.info("Configuring {} for the output  {} with partition strategy {}", this.getClass().getName(), dataSinkContextName,
                strategy.getClass().getName());
        strategy.init(properties);
        LOGGER.trace("Finished configureDataSink method for the sink context name {} ", dataSinkContextName);
    }

    /**
     * Writes Dataset to Hive table based on Partition strategy
     */
    @Override
    public void write(final Dataset dataset) {
        LOGGER.trace("Initiating Write for {}. ", getDataSinkContextName());
        final String ddl = TransformerUtils.buildDDL(dataset.schema(), pColumns, getWritingContext(),
                (format == null || format.trim().length() < 0) ? "PARQUET" : format);
        LOGGER.trace("Creating hive table {}", ddl);
        LOGGER.info("Loading data into hive table {}", getWritingContext());
        final Dataset sql = ((SQLContext) getContext()).sql(ddl);
        if (sql.count() == 0) {
            //  //https://www.mail-archive.com/commits@spark.apache.org/msg21079.html
            dataset.write().mode(SaveMode.Append).insertInto(getWritingContext());
        }
        LOGGER.trace("Finished Write for {}. ", getDataSinkContextName());
    }

    /**
     * Gets the p columns.
     *
     * @return the p columns
     */
    public String[] getpColumns() {
        return pColumns;
    }

    /**
     * Sets the p columns.
     *
     * @param pColumns
     *            the new p columns
     */
    public void setpColumns(final String[] pColumns) {
        this.pColumns = pColumns;
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
        return IOURIS.HIVE.getUri();
    }
}
