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
package com.ericsson.component.aia.services.bps.spark.jobrunner;

import static com.ericsson.component.aia.services.bps.core.common.Constants.*;
import static com.ericsson.component.aia.services.bps.core.common.ExecutionMode.*;
import static com.ericsson.component.aia.services.bps.spark.common.Constants.*;

import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.services.bps.core.common.Constants;
import com.ericsson.component.aia.services.bps.core.common.uri.IOURIS;
import com.ericsson.component.aia.services.bps.core.common.uri.PROCESS_URIS;
import com.ericsson.component.aia.services.bps.core.service.BpsJobRunner;
import com.ericsson.component.aia.services.bps.core.service.configuration.BpsDataStreamsConfigurer;
import com.ericsson.component.aia.services.bps.core.service.configuration.datasink.BpsDataSinkAdapters;
import com.ericsson.component.aia.services.bps.core.service.configuration.datasink.BpsDataSinkConfiguration;
import com.ericsson.component.aia.services.bps.core.service.configuration.datasource.BpsDataSourceAdapters;
import com.ericsson.component.aia.services.bps.core.service.configuration.datasource.BpsDataSourceConfiguration;
import com.ericsson.component.aia.services.bps.core.service.streams.BpsInputStreams;
import com.ericsson.component.aia.services.bps.core.service.streams.BpsOutputSinks;
import com.ericsson.component.aia.services.bps.core.utils.PropertiesReader;
import com.ericsson.component.aia.services.bps.spark.jobrunner.common.SparkSessionHelper;

/**
 * SparkSQLHandler class is a one of the implementation for Step interface. This handler is used when the user wants to run SQL query scenario using
 * Spark batch (With Hive Context).
 */
public abstract class BpsSparkBatchJobRunner implements BpsJobRunner {

    /** Logger. */
    private static final Logger LOG = LoggerFactory.getLogger(BpsSparkBatchJobRunner.class);

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 655833713003724650L;

    /** The out going. */
    private BpsOutputSinks outGoing;

    /** The properties. */
    private Properties stepProperties;

    /** The streams. */
    private transient BpsInputStreams bpsInputStreams;

    private BpsDataSourceAdapters inputAdapters;

    private BpsDataSinkAdapters outputAdapters;

    /**
     * Initializes step handler parameters.
     *
     * @param inputAdapters
     *            the input adapters
     * @param outputAdapters
     *            the output adapters
     * @param properties
     *            the properties
     */
    @Override
    public void initialize(final BpsDataSourceAdapters inputAdapters, final BpsDataSinkAdapters outputAdapters, final Properties properties) {

        LOG.trace("Entering the initialize method");
        this.stepProperties = properties;
        this.inputAdapters = inputAdapters;
        this.outputAdapters = outputAdapters;
        LOG.trace("Exiting the initialize method");
    }

    /**
     * Execute method runs Step of a pipeline job.
     */
    @Override
    public void execute() {
        LOG.trace("Entering the execute method");
        initializeContextAndChannels();
        executeJob();
        //getJavaSparkContext().stop();
        LOG.trace("Exiting the execute method");
    }

    /**
     * executeJob operation should be implemented by the respective streaming driver class.
     */
    @Override
    public abstract void executeJob();

    /**
     * This operation will do the clean up for Job's Step handlers.
     */
    @Override
    public void cleanUp() {
        LOG.trace("Entering the cleanUp method");
        outGoing.cleanUp();
        SparkSessionHelper.closeSparkSession();
        LOG.trace("Exiting the cleanUp method");
    }

    /**
     * Will initializeContextAndChannels.
     */
    private void initializeContextAndChannels() {
        SparkSessionHelper.initializeSparkContext(inputAdapters, outputAdapters, stepProperties);
        checkForLocalMode(inputAdapters, outputAdapters);
        bpsInputStreams = BpsDataStreamsConfigurer.populateBpsInputStreams(inputAdapters, SparkSessionHelper.getSession().sqlContext());
        outGoing = BpsDataStreamsConfigurer.populateBpsOutputStreams(outputAdapters, SparkSessionHelper.getSession().sqlContext());
    }

    /**
     * Check for local mode and validates io adapters for local mode.
     */
    private void checkForLocalMode(final BpsDataSourceAdapters inputAdapters, final BpsDataSinkAdapters outputAdapters) {

        if (stepProperties.containsKey(MASTER_URL) && !StringUtils.startsWithIgnoreCase(stepProperties.getProperty(MASTER_URL).trim(), SPARK_URI)
                && !(stepProperties.containsKey(MODE) && StringUtils.equalsIgnoreCase(stepProperties.getProperty(MODE).trim(), EMBEDDED.name()))) {

            for (final BpsDataSourceConfiguration in : inputAdapters.getBpsDataSourceConfigurations()) {
                final Properties configuration = in.getDataSourceConfiguration();
                final String property = configuration.getProperty(Constants.URI);
                if (IOURIS.findUri(property) != IOURIS.FILE) {
                    throw new UnsupportedOperationException("Supports only File input and output operation in local mode.");
                }
            }

            for (final BpsDataSinkConfiguration out : outputAdapters.getBpsDataSinkConfigurations()) {
                final Properties configuration = out.getDataSinkConfiguration();
                final String property = configuration.getProperty(Constants.URI);

                if (IOURIS.findUri(property) != IOURIS.FILE) {
                    throw new UnsupportedOperationException("Supports only File input and output operation in local mode.");
                }

            }
        }

    }

    /**
     * Gets the service context name.
     *
     * @return the service context name
     */
    @Override
    public String getServiceContextName() {
        return PROCESS_URIS.SPARK_BATCH.getUri();
    }

    /**
     * Gets the bps input streams.
     *
     * @return the bps input streams
     */
    public BpsInputStreams getBpsInputStreams() {
        return bpsInputStreams;
    }

    /**
     * To string.
     *
     * @return the string
     */
    @Override
    public String toString() {
        return "SparkSQLHandler [properties=" + PropertiesReader.getPropertyAsString(stepProperties) + "]";
    }

    /**
     * Gets the step properties.
     *
     * @return the properties
     */
    public Properties getStepProperties() {
        return stepProperties;
    }

    /**
     * Gets the outGoing.
     *
     * @return the outGoing
     */
    public BpsOutputSinks getOutGoing() {
        return outGoing;
    }
}
