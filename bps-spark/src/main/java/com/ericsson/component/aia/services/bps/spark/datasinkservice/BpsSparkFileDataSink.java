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

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLEncoder;
import java.util.concurrent.TimeUnit;

import org.apache.spark.sql.Dataset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.services.bps.core.common.Constants;
import com.ericsson.component.aia.services.bps.core.common.uri.IOURIS;
import com.ericsson.component.aia.services.bps.core.configuration.rule.impl.BpsFileUriFormatRule;
import com.ericsson.component.aia.services.bps.core.service.configuration.datasink.BpsDataSinkConfiguration;
import com.ericsson.component.aia.services.bps.core.service.partition.BpsPartition;
import com.ericsson.component.aia.services.bps.core.service.streams.BpsAbstractDataSink;
import com.ericsson.component.aia.services.bps.spark.configuration.partition.SparkDefaultPartition;

/**
 * The <code>BpsSparkFileDataSink</code> class is responsible for writing {@link Dataset } to a file.<br>
 * The default Partition strategy is {@link SparkDefaultPartition}.
 *
 * @param <C>
 *            the generic type representing the context like HiveContext etc.
 */
@SuppressWarnings("rawtypes")
public class BpsSparkFileDataSink<C> extends BpsAbstractDataSink<C, Dataset> {

    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger(BpsSparkFileDataSink.class);

    /** The Partition strategy used by the FileDataSink. */
    @SuppressWarnings("rawtypes")
    private BpsPartition<Dataset> strategy;

    /** The method. */
    private TimeUnit method;

    /** The data format. */
    private String dataFormat;

    /**
     * Configured instance of {@link BpsSparkFileDataSink} for the specified sinkContextName.<br>
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
        strategy = new SparkDefaultPartition();
        LOGGER.info("Configuring %s for the output  {} with partition strategy {}", this.getClass().getName(), dataSinkContextName,
                strategy.getClass().getName());
        strategy.init(properties);
        LOGGER.trace("Finished configureDataSink method for the sink context name {} ", dataSinkContextName);
    }

    /**
     * The method will try to delete all the files and folder recursively starting from the folder/file name provided to the method. The
     * {@link BpsSparkFileDataSink#write(Dataset)} methods uses {@link BpsSparkFileDataSink#delete(File)} methods in order to delete the files and
     * folder before writing new data to the provided path.
     *
     * @param parentFolder
     *            Try to delete all the files belongs to parent folder.
     */
    void delete(final File parentFolder) {

        LOGGER.trace("Try to delete {}", parentFolder.getAbsolutePath());
        if (parentFolder.isDirectory()) {
            for (final File childFile : parentFolder.listFiles()) {
                delete(childFile);
            }
        }
        if (!parentFolder.delete()) {
            throw new IllegalStateException(String.format("Failed to deleted %s ", parentFolder.getAbsolutePath()));
        }
        LOGGER.trace("Delete successfully {} ", parentFolder.getAbsolutePath());
    }

    /**
     * Writes Dataset to file location based on Partition strategy
     */
    @Override
    public void write(final Dataset dataSet) {
        LOGGER.trace("Initiating Write for {}. ", getDataSinkContextName());
        File file = null;
        try {
            file = new File(URI.create(URLEncoder.encode(getProperties().getProperty(Constants.URI), "UTF-8")).getPath());
            if (file.exists()) {
                delete(file);
            }
        } catch (final UnsupportedEncodingException e) {
            LOGGER.error("Caught Error while encoding URL  ", e);
        }
        strategy.write(dataSet, IOURIS.FILE.getUri() + getWritingContext());
        LOGGER.trace("Finished Write for {}. ", getDataSinkContextName());
    }

    /**
     * Gets the method.
     *
     * @return the method
     */
    public TimeUnit getMethod() {
        return method;
    }

    /**
     * Sets the method of unit.
     *
     * @param method
     *            the new method
     */
    public void setMethod(final TimeUnit method) {
        this.method = method;
    }

    /**
     * Gets the data format.
     *
     * @return the dataFormat
     */
    public String getDataFormat() {
        return dataFormat;
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
        return IOURIS.FILE.getUri();
    }

    @Override
    public String getUriRegex() {
        return BpsFileUriFormatRule.getRegex();
    }
}
