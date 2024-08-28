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
package com.ericsson.component.aia.services.bps.flink.datasinkservice;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.services.bps.core.common.Constants;
import com.ericsson.component.aia.services.bps.core.common.uri.IOURIS;
import com.ericsson.component.aia.services.bps.core.configuration.rule.impl.BpsFileUriFormatRule;
import com.ericsson.component.aia.services.bps.core.service.configuration.datasink.BpsDataSinkConfiguration;
import com.ericsson.component.aia.services.bps.core.service.streams.BpsAbstractDataSink;

/**
 * The <code>BpsFlinkFileDataSink</code> class is responsible for writing DataFrame to a file.<br>
 *
 * @param <C>
 *            the generic type representing the context like {@link StreamExecutionEnvironment} etc.
 */
public class BpsFlinkFileDataSink<C> extends BpsAbstractDataSink<C, DataStream<?>> {

    private static final String SEPARATOR = File.separator;

    private static final String TEXT_DATA_FORMAT = "txt";

    private static final String CSV_DATA_FORMAT = "csv";

    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger(BpsFlinkFileDataSink.class);

    private final Calendar cal = GregorianCalendar.getInstance(TimeZone.getTimeZone("UTC"));

    /** The method. */
    private TimeUnit method;

    /** The data format. */
    private String dataFormat;

    private WriteMode writeMode;

    /**
     * Configured instance of {@link BpsFlinkFileDataSink} for the specified sinkContextName.<br>
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
        LOGGER.trace("ConfigureDataSink for context={} ", bpsDataSinkConfiguration.getDataSinkContextName());
        super.configureDataSink(context, bpsDataSinkConfiguration);
        dataFormat = properties.getProperty("data.format");
        final String saveMode = properties.getProperty(Constants.SAVE_MODE);
        writeMode = getWriteMode(saveMode);
        LOGGER.trace("Finished configureDataSink method for the sink context name {}", bpsDataSinkConfiguration.getDataSinkContextName());
    }

    /**
     * The method will try to delete all the files and folder recursively starting from the folder/file name provided to the method. The
     * {@link BpsFlinkFileDataSink#write(DataFrame)} methods uses {@link BpsFlinkFileDataSink#delete(File)} methods in order to delete the files and
     * folder before writing new data to the provided path.
     *
     * @param parentFolder
     *            Try to delete all the files belongs to parent folder.
     */
    void delete(final File parentFolder) {

        LOGGER.trace(String.format("Try to delete %s ", parentFolder.getAbsolutePath()));
        if (parentFolder.isDirectory()) {
            for (final File childFile : parentFolder.listFiles()) {
                delete(childFile);
            }
        }
        if (!parentFolder.delete()) {
            throw new IllegalStateException(String.format("Failed to deleted %s ", parentFolder.getAbsolutePath()));
        }
        LOGGER.trace(String.format("Delete successfully %s ", parentFolder.getAbsolutePath()));
    }

    /**
     * Writes DataFrame to file location based on Partition strategy
     */
    @Override
    public void write(final DataStream<?> dataSet) {
        LOGGER.trace("Initiating Write for {}", getDataSinkContextName());
        if (dataSet == null) {
            LOGGER.info("Record null returning...");
            return;
        }
        final String dataFormat = getProperties().getProperty("data.format");
        LOGGER.info("Record not null writing now...");
        final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        final String dateAsString = format.format(cal.getTime()) + SEPARATOR;
        if (TEXT_DATA_FORMAT.equalsIgnoreCase(dataFormat)) {
            dataSet.writeAsText(getWritingContext() + SEPARATOR + dateAsString + SEPARATOR, writeMode).setParallelism(1);
        } else if (CSV_DATA_FORMAT.equalsIgnoreCase(dataFormat)) {
            dataSet.writeAsCsv(getWritingContext() + SEPARATOR + dateAsString + SEPARATOR, writeMode);
        }
        LOGGER.trace(String.format("Finished Write for %s. ", getDataSinkContextName()));
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

    /**
     * Cleans up the allocated resources for the data sink.
     */
    @Override
    public void cleanUp() {
        LOGGER.trace("Cleaning resources allocated for {} ", getDataSinkContextName());
        strategy = null;
        LOGGER.trace("Cleaned resources allocated for {} ", getDataSinkContextName());
    }

    /**
     * Returns the service context name.
     */
    @Override
    public String getServiceContextName() {
        return IOURIS.FILE.getUri();
    }

    @Override
    public String getUriRegex() {
        return BpsFileUriFormatRule.getRegex();
    }

    private WriteMode getWriteMode(final String saveMode) {
        if (saveMode == null) {
            return WriteMode.NO_OVERWRITE;
        } else if ("Overwrite".equalsIgnoreCase(saveMode)) {
            return WriteMode.OVERWRITE;
        } else {
            return WriteMode.NO_OVERWRITE;
        }
    }

}
