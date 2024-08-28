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
package com.ericsson.component.aia.services.bps.spark.jobrunner.common;

import static com.ericsson.component.aia.model.registry.utils.Constants.SCHEMA_REGISTRY_ADDRESS_PARAMETER;
import static com.ericsson.component.aia.model.registry.utils.Constants.SCHEMA_REGISTRY_CACHE_MAX_SIZE_PARAMETER;
import static com.ericsson.component.aia.services.bps.core.common.Constants.APP_NAME;
import static com.ericsson.component.aia.services.bps.core.common.Constants.MASTER_URL;
import static com.ericsson.component.aia.services.bps.spark.common.Constants.SLIDE_WINDOW_LENGTH;
import static com.ericsson.component.aia.services.bps.spark.common.Constants.WINDOW_LENGTH;
import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.spark.launcher.SparkLauncher.DRIVER_EXTRA_JAVA_OPTIONS;
import static org.apache.spark.launcher.SparkLauncher.EXECUTOR_EXTRA_JAVA_OPTIONS;

import java.io.Serializable;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession.Builder;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.services.bps.core.common.Constants;
import com.ericsson.component.aia.services.bps.core.common.uri.IOURIS;
import com.ericsson.component.aia.services.bps.core.service.configuration.datasink.BpsDataSinkAdapters;
import com.ericsson.component.aia.services.bps.core.service.configuration.datasink.BpsDataSinkConfiguration;
import com.ericsson.component.aia.services.bps.core.service.configuration.datasource.BpsDataSourceAdapters;
import com.ericsson.component.aia.services.bps.core.service.configuration.datasource.BpsDataSourceConfiguration;
import com.ericsson.component.aia.services.bps.spark.configuration.KafkaStreamConfiguration;

/**
 * The <code>SparkSessionHelper</code> is responsible for managing spark related operations.
 */
public class SparkSessionHelper implements Serializable {

    /**
     * The Constant serialVersionUID.
     */
    private static final long serialVersionUID = -7684414561487437994L;

    private static final Pattern SPARK_EXTRA_CONF = Pattern.compile("^\\s*spark\\..+", Pattern.CASE_INSENSITIVE);

    /**
     * Logger.
     */
    private static final Logger LOG = LoggerFactory.getLogger(SparkSessionHelper.class);

    /**
     * The jssc.
     */
    private static transient JavaStreamingContext jssc;

    /**
     * The session.
     */
    private static SparkSession session;

    /**
     * Instantiates a new spark app header.
     */
    private SparkSessionHelper() {

    }

    /**
     * Gets the session.
     *
     * @return the session
     */
    public static SparkSession getSession() {
        return session;
    }

    /**
     * A method to create kafka stream.
     *
     * @param <K>
     *            the key type
     * @param <V>
     *            the value type
     * @param streamingParameters
     *            the streaming parameters
     * @return instance of {@link JavaPairInputDStream}
     * @throws ClassNotFoundException
     *             the class not found exception
     */
    public static <K, V> JavaDStream<ConsumerRecord<K, V>> createKafkaConnection(final Properties streamingParameters) throws ClassNotFoundException {
        LOG.trace("Entering the createConnection method");

        final KafkaStreamConfiguration conf = new KafkaStreamConfiguration(streamingParameters);

        final JavaInputDStream<ConsumerRecord<K, V>> stream = KafkaUtils.createDirectStream(jssc, LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<K, V> Subscribe(conf.getTopicsSet(), conf.getKafkaParams()));

        if (streamingParameters.containsKey(WINDOW_LENGTH)) {

            final String windowLen = streamingParameters.getProperty(WINDOW_LENGTH);
            checkArgument(NumberUtils.isNumber(windowLen), WINDOW_LENGTH + " is not a number " + windowLen);
            final Long windowLength = NumberUtils.toLong(windowLen);

            if (streamingParameters.containsKey(SLIDE_WINDOW_LENGTH)) {
                final String slideLen = streamingParameters.getProperty(SLIDE_WINDOW_LENGTH);
                checkArgument(NumberUtils.isNumber(slideLen), SLIDE_WINDOW_LENGTH + " is not a number " + slideLen);
                final Long slideWindow = NumberUtils.toLong(slideLen);
                return stream.window(Durations.milliseconds(windowLength), Durations.milliseconds(slideWindow));
            } else {
                return stream.window(Durations.milliseconds(windowLength));
            }
        }
        LOG.trace("returning the createConnection method");
        return stream;
    }

    /**
     * This method creates basic context for the spark.
     *
     * @param bpsInput
     *            is the mapped inputs from flow xml file
     * @param bpsOut
     *            is the mapped output from the flow xml file
     * @param sparkJobConfig
     *            the properties
     */
    @SuppressWarnings("unchecked")
    public static void initializeSparkContext(final BpsDataSourceAdapters bpsInput, final BpsDataSinkAdapters bpsOut,
                                              final Properties sparkJobConfig) {
        LOG.trace("Initializing spark context()-->");

        final Builder sparkSessionBuilder = initSparkConf(sparkJobConfig, isHiveSupportRequired(bpsInput, bpsOut));
        session = sparkSessionBuilder.getOrCreate();
        LOG.trace("Initializing spark context()<--");
    }

    /**
     * Inits the spark conf.
     *
     * @param sparkJobConfig
     *            the spark job config
     * @return the builder
     */
    //     */
    private static Builder initSparkConf(final Properties sparkJobConfig, final boolean enabledHiveSupport) {

        final Builder sparkSessionBuilder = SparkSession.builder();

        if (sparkJobConfig.containsKey(APP_NAME)) {
            sparkSessionBuilder.appName(sparkJobConfig.getProperty(APP_NAME));
        }

        if (sparkJobConfig.containsKey(MASTER_URL)) {
            sparkSessionBuilder.master(sparkJobConfig.getProperty(MASTER_URL));
        }

        if (enabledHiveSupport) {
            sparkSessionBuilder.enableHiveSupport();
        }

        updateSparkConfig(sparkJobConfig, sparkSessionBuilder);
        return sparkSessionBuilder;

    }

    /**
     * Check hive support required.
     */
    private static boolean isHiveSupportRequired(final BpsDataSourceAdapters inputAdapters, final BpsDataSinkAdapters outputAdapters) {

        if (null != inputAdapters) {
            for (final BpsDataSourceConfiguration in : inputAdapters.getBpsDataSourceConfigurations()) {
                final Properties configuration = in.getDataSourceConfiguration();
                final String property = configuration.getProperty(Constants.URI);
                if (IOURIS.findUri(property) == IOURIS.HIVE) {
                    return true;
                }

            }
        }
        if (null != outputAdapters) {
            for (final BpsDataSinkConfiguration out : outputAdapters.getBpsDataSinkConfigurations()) {
                final Properties configuration = out.getDataSinkConfiguration();
                final String property = configuration.getProperty(Constants.URI);
                if (IOURIS.findUri(property) == IOURIS.HIVE) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * A method to initialize the spark streaming context using the components provided.
     *
     * @param bpsInput
     *            is the mapped inputs from flow xml file
     * @param bpsOut
     *            is the mapped output from the flow xml file
     * @param sparkJobConfig
     *            the properties
     * @return instant of {@link JavaStreamingContext}
     */
    public static JavaStreamingContext initializeStreamingContext(final BpsDataSourceAdapters bpsInput, final BpsDataSinkAdapters bpsOut,
                                                                  final Properties sparkJobConfig) {

        LOG.trace("Entering the ProxyHandler method");
        final Builder sparkSessionBuilder = initSparkConf(sparkJobConfig, isHiveSupportRequired(bpsInput, bpsOut));
        LOG.trace("Creating SparkSession");
        final long windowLength = Long.parseLong(sparkJobConfig.getProperty(WINDOW_LENGTH, "1000"));
        final Duration batchInterval = new Duration(windowLength);

        LOG.info("Initializing the Java Spark Streaming Context with batch interval of {} mS", windowLength);
        if (sparkJobConfig.containsKey("streaming.checkpoint")) {
            LOG.info("Spark checkpoint directory provided {}. Trying to create/restore stream based on last known status",
                    sparkJobConfig.getProperty("streaming.checkpoint"));
            jssc = JavaStreamingContext.getOrCreate(sparkJobConfig.getProperty("streaming.checkpoint"), new Function0<JavaStreamingContext>() {
                private static final long serialVersionUID = 1L;

                @SuppressWarnings("PMD.SignatureDeclareThrowsException")
                @Override
                public JavaStreamingContext call() throws Exception {
                    LOG.info("SparkStreaming context created using create function");
                    session = sparkSessionBuilder.getOrCreate();
                    jssc = new JavaStreamingContext(new StreamingContext(session.sparkContext(), batchInterval));
                    jssc.checkpoint(sparkJobConfig.getProperty("streaming.checkpoint"));
                    return jssc;
                }
            });

        } else {
            LOG.info("No Checkpoint directory information provided. Hence creating context without checkpoint ");
            session = sparkSessionBuilder.getOrCreate();
            jssc = new JavaStreamingContext(new StreamingContext(session.sparkContext(), batchInterval));
        }

        if (session == null) {
            // when jssc created from checkpoint dir
            session = new SparkSession(jssc.sparkContext().sc());
        }
        LOG.trace("Initializing Spark Streaming context()-->");
        return jssc;
    }

    /**
     * Helps to set the spark configuration and will check the schema registry parameters
     *
     * @param sparkJobConfig
     *            the spark job config
     * @param sparkSessionBuilder
     *            the spark session builder
     */
    @SuppressWarnings("PMD.NPathComplexity")
    private static void updateSparkConfig(final Properties sparkJobConfig, final Builder sparkSessionBuilder) {

        final String SYSTEM_PARAMETER = " -D";
        final StringBuilder configParam = new StringBuilder("");

        if (sparkJobConfig.containsKey(SCHEMA_REGISTRY_ADDRESS_PARAMETER)) {
            configParam.append(
                    SYSTEM_PARAMETER + SCHEMA_REGISTRY_ADDRESS_PARAMETER + "=" + sparkJobConfig.getProperty(SCHEMA_REGISTRY_ADDRESS_PARAMETER));
        }

        if (sparkJobConfig.containsKey(SCHEMA_REGISTRY_CACHE_MAX_SIZE_PARAMETER)) {
            configParam.append(SYSTEM_PARAMETER + SCHEMA_REGISTRY_CACHE_MAX_SIZE_PARAMETER + "="
                    + sparkJobConfig.getProperty(SCHEMA_REGISTRY_CACHE_MAX_SIZE_PARAMETER));
        }

        for (final Object key : sparkJobConfig.keySet()) {
            final String extrSparkConfKey = key.toString().trim();
            final String value = sparkJobConfig.getProperty(extrSparkConfKey);
            if (SPARK_EXTRA_CONF.matcher(extrSparkConfKey).matches() && StringUtils.isNoneBlank(value)) {
                sparkSessionBuilder.config(extrSparkConfKey, value);
                LOG.info("Found extra SparkConf property {} = {}", extrSparkConfKey, value);
            }
        }

        final String passedExecutorParams = StringUtils.isNoneBlank(System.getProperty(EXECUTOR_EXTRA_JAVA_OPTIONS))
                ? " " + System.getProperty(EXECUTOR_EXTRA_JAVA_OPTIONS) : "";
        final String passedDriverParams = StringUtils.isNoneBlank(System.getProperty(DRIVER_EXTRA_JAVA_OPTIONS))
                ? " " + System.getProperty(DRIVER_EXTRA_JAVA_OPTIONS) : "";

        sparkSessionBuilder.config(EXECUTOR_EXTRA_JAVA_OPTIONS, configParam + passedExecutorParams);
        sparkSessionBuilder.config(DRIVER_EXTRA_JAVA_OPTIONS, configParam + passedDriverParams);

    }

    /**
     * This method is responsible to closing JavaStreamingContext
     */
    private static void closeJavaStreamingContext() {
        if (jssc != null) {
            LOG.info("Closing JavaStreamingContext");
            jssc.stop();
            jssc.close();
            jssc = null;
        }
    }

    /**
     * This method is responsible to closing Spark Session
     */
    public static void closeSparkSession() {
        closeJavaStreamingContext();
        if (session != null) {
            LOG.info("Closing Spark Session");
            SparkSession.clearActiveSession();
            SparkSession.clearDefaultSession();
            session.close();
            session = null;
        }
    }

}
