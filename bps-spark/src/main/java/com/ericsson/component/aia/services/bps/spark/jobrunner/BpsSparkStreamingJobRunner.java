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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.services.bps.core.common.uri.PROCESS_URIS;
import com.ericsson.component.aia.services.bps.core.exception.BpsRuntimeException;
import com.ericsson.component.aia.services.bps.core.service.BpsJobRunner;
import com.ericsson.component.aia.services.bps.core.service.configuration.BpsDataStreamsConfigurer;
import com.ericsson.component.aia.services.bps.core.service.configuration.datasink.BpsDataSinkAdapters;
import com.ericsson.component.aia.services.bps.core.service.configuration.datasource.BpsDataSourceAdapters;
import com.ericsson.component.aia.services.bps.core.service.streams.BpsInputStreams;
import com.ericsson.component.aia.services.bps.core.service.streams.BpsOutputSinks;
import com.ericsson.component.aia.services.bps.spark.common.avro.AvroToSqlConverterUtil;
import com.ericsson.component.aia.services.bps.spark.jobrunner.common.SparkSessionHelper;

/**
 * SparkStreamingHandler class is a base class and one of the implementation for Step interface. This handler is used when the user wants to run
 * Streaming application using Spark DStream.
 */
public abstract class BpsSparkStreamingJobRunner implements BpsJobRunner {

    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger(BpsSparkStreamingJobRunner.class);

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 655833713003724650L;

    /** Input sources for streaming job. */
    protected transient BpsInputStreams inputStreams;

    /** Output sinks for streaming job. */
    protected transient BpsOutputSinks outGoingStreams;

    /** Holds/Refers to Spark Streaming context. */
    protected transient JavaStreamingContext jssc;

    /** The properties. */
    protected Properties stepProperties;

    /** The input adapters. */
    private transient BpsDataSourceAdapters inputAdapters;

    /** The output adapters. */
    private transient BpsDataSinkAdapters outputAdapters;

    /** Optional parameter for Spark Streaming context to timeout. */
    private transient Long timeout;

    /**
     * Execute() operation calls driver class job as in defined in the Job pipeline.
     */
    @Override
    public void execute() {

        LOGGER.trace("Started execute() operation SparkStreamingJobRunner");
        initializeContextAndChannels();
        if (!jssc.ssc().isCheckpointPresent()) {
            executeJob();
        }
        jssc.start();

        if (null != timeout) {
            try {
                jssc.awaitTerminationOrTimeout(timeout);
            } catch (final InterruptedException e) {
                throw new BpsRuntimeException(e);
            }
        } else {
            try {
                jssc.awaitTermination();
            } catch (final InterruptedException e) {
                throw new BpsRuntimeException(e);
            }
        }

        LOGGER.trace("Finished execute() operation SparkStreamingJobRunner");
    }

    /**
     * executeJob operation should be implemented by the respective streaming driver class.
     */
    @Override
    public abstract void executeJob();

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

        LOGGER.trace("Entering the initialize method");
        this.inputAdapters = inputAdapters;
        this.outputAdapters = outputAdapters;
        this.stepProperties = properties;
        LOGGER.trace("Exiting the initialize method");
    }

    /**
     * Gets the service context name.
     *
     * @return the service context name
     */
    @Override
    public String getServiceContextName() {
        return PROCESS_URIS.SPARK_STREAMING.getUri();
    }

    /**
     * Clean up.
     */
    @Override
    public void cleanUp() {
        LOGGER.trace("Started cleanUp action");

        if (getInputStreams() != null) {
            LOGGER.trace("Cleaning resources allocated for inputs");
            getInputStreams().cleanUp();
        }
        if (getOutGoingStreams() != null) {
            LOGGER.trace("Cleaning resources allocated for outputs");
            getOutGoingStreams().cleanUp();
        }
        LOGGER.trace("Closing spark session");
        SparkSessionHelper.closeSparkSession();
        jssc = null;
        LOGGER.trace("Finished cleanUp action");
    }

    /**
     * Save JavaRDD to the respective sinks as declared in flow xml.
     *
     * @param <T>
     *            the generic type
     * @param records
     *            the records
     */
    protected <T> void persist(final JavaRDD<T> records) {

        if (null != records && !records.isEmpty()) {
            final Class class1 = records.classTag().getClass();
            final Encoder bean = Encoders.bean(class1);
            final Dataset dataset = SparkSessionHelper.getSession().sqlContext().createDataset(records.rdd(), bean);
            outGoingStreams.write(dataset);
        }

    }

    /**
     * Save Dataset to the respective sinks as declared in flow xml.
     *
     * @param dataset
     *            the data frame
     */
    protected void persist(final Dataset dataset) {
        outGoingStreams.write(dataset);
    }

    /**
     * Save Generic records to the respective sinks as declared in flow xml.
     *
     * @param genericRecords
     *            the generic records
     */
    @SuppressWarnings("deprecation")
    protected void persist(final JavaDStream<GenericRecord> genericRecords) {

        genericRecords.foreachRDD(new VoidFunction<JavaRDD<GenericRecord>>() {

            private static final long serialVersionUID = -5248934452348721313L;

            @Override
            public void call(final JavaRDD<GenericRecord> genericRecordRdd) {

                if (!genericRecordRdd.isEmpty()) {

                    saveGenericRecords(genericRecordRdd);
                }
            }
        });
    }

    /**
     * Initialize context and channels.
     */
    private void initializeContextAndChannels() {

        if (jssc == null) {
            jssc = SparkSessionHelper.initializeStreamingContext(inputAdapters, outputAdapters, stepProperties);
        }
        if (!jssc.ssc().isCheckpointPresent()) {
            LOGGER.info("BPS uses standard configuration to configure the streaming context");
            inputStreams = BpsDataStreamsConfigurer.populateBpsInputStreams(inputAdapters, SparkSessionHelper.getSession().sqlContext());
        }

        outGoingStreams = BpsDataStreamsConfigurer.populateBpsOutputStreams(outputAdapters, SparkSessionHelper.getSession().sqlContext());

        if (stepProperties.containsKey("timeout")) {
            timeout = Long.parseLong(stepProperties.getProperty("timeout"));
        }
    }

    /**
     * Save generic records.
     *
     * @param genericRecordRdd
     *            the generic record rdd
     */
    public void saveGenericRecords(final JavaRDD<GenericRecord> genericRecordRdd) {

        final Schema schema = genericRecordRdd.first().getSchema();

        // Generate the schema based on the string of schema
        final List<StructField> fields = new ArrayList<>();

        for (final Field field : schema.getFields()) {
            final DataType dataType = AvroToSqlConverterUtil.toSqlType(field.schema());
            fields.add(DataTypes.createStructField(field.name(), dataType, true));
        }

        final StructType structSchema = DataTypes.createStructType(fields);

        // Convert records of the RDD (people) to Rows
        final JavaRDD<Row> rowRDD = genericRecordRdd.map(new Function<GenericRecord, Row>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Row call(final GenericRecord record) {

                final List<Object> attributes = new ArrayList<>(fields.size());
                for (final StructField structField : fields) {
                    attributes.add(AvroToSqlConverterUtil.createConverterToSQL(record, structField));
                }

                return RowFactory.create(attributes.toArray());
            }
        });

        // Apply the schema to the RDD
        final Dataset genericRecordDF = SparkSessionHelper.getSession().sqlContext().createDataFrame(rowRDD, structSchema);
        outGoingStreams.write(genericRecordDF);
    }

    /**
     * @return the inputStreams
     */
    public BpsInputStreams getInputStreams() {
        return inputStreams;
    }

    /**
     * @return the outGoingStreams
     */
    public BpsOutputSinks getOutGoingStreams() {
        return outGoingStreams;
    }

    /**
     * @return the stepProperties
     */
    public Properties getStepProperties() {
        return stepProperties;
    }
}
