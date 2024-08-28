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
package com.ericsson.component.aia.services.bps.spark.kafka;

import static com.ericsson.component.aia.model.registry.utils.Constants.SCHEMA_REGISTRY_ADDRESS_PARAMETER;
import static com.ericsson.component.aia.model.registry.utils.Constants.SCHEMA_REGISTRY_CACHE_MAX_SIZE_PARAMETER;
import static com.ericsson.component.aia.services.bps.spark.common.avro.SparkRddToAvro.transformRddToAvroRdd;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.common.transport.kafka.writer.api.KafkaKeyGenerator;
import com.ericsson.component.aia.common.transport.kafka.writer.builder.KafkaGenericWriterBuilder;
import com.ericsson.component.aia.common.transport.kafka.writer.impl.KafkaGenericWriter;
import com.ericsson.component.aia.model.registry.client.SchemaRegistryClient;
import com.ericsson.component.aia.model.registry.exception.SchemaRetrievalException;
import com.ericsson.component.aia.model.registry.impl.RegisteredSchema;
import com.ericsson.component.aia.model.registry.impl.SchemaRegistryClientFactory;
import com.ericsson.component.aia.model.registry.utils.Utils;
import com.ericsson.component.aia.services.bps.core.exception.GenericRecordConversionException;
import com.ericsson.component.aia.services.bps.spark.configuration.KafkaStreamConfiguration;
import com.ericsson.component.aia.services.bps.spark.datasinkservice.BpsSparkKafkaDataSink;
import com.ericsson.component.aia.services.bps.spark.datasinkservice.DataWriter;

/**
 * The Class AvroDataFrameWriter is used to write data frames to KAFKA as Avro generic records.
 */
@SuppressWarnings("rawtypes")
public class AvroDatasetWriter extends DataWriter<Dataset> {
    /** serialVersionUID. */
    private static final long serialVersionUID = -4916428491473581453L;

    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger(BpsSparkKafkaDataSink.class);

    /** The writer. */
    private final KafkaGenericWriter<String, GenericRecord> writer;

    /** The schema registry url. */
    private final String schemaRegistryUrl;

    /** The schema registry cache. */
    private final int schemaRegistryCache;

    /** The lookup. */
    private transient RegisteredSchema lookup;

    /**
     * Instantiates a new avro data frame writer.
     *
     * @param properties
     *            the properties
     * @param topic
     *            the topic
     */
    public AvroDatasetWriter(final Properties properties, final String topic) {
        super.setSinkProperties(properties);
        this.schemaRegistryUrl = Utils.getSchemaRegistryUrlProperty(properties);
        this.schemaRegistryCache = Utils.getSchemaRegistryCacheSizeProperty(properties);

        try {
            this.lookup = getEventSchema(properties);
        } catch (final IllegalArgumentException exception) {
            LOGGER.warn("Unable to retrieve event type: {} from schema registry. It will retry while writing the record.",
                    properties.getProperty("eventType"));
        }

        final KafkaStreamConfiguration kafkaStreamConfiguration = new KafkaStreamConfiguration(properties);
        final KafkaGenericWriterBuilder<String, GenericRecord> kafkaGenericWriterBuilder = KafkaGenericWriterBuilder.create();

        final Map<String, Object> kafkaParams = kafkaStreamConfiguration.getKafkaParams();

        final Map<String, String> kafkaStringParams = new HashMap<String, String>();

        for (final String key : kafkaParams.keySet()) {
            kafkaStringParams.put(key, (String) kafkaParams.get(key));
        }
        kafkaGenericWriterBuilder.withBrokers(kafkaStreamConfiguration.getBrokers()).withKeySerializer(kafkaStreamConfiguration.getKeySerializer())
                .withValueSerializer(kafkaStreamConfiguration.getValueSerializer()).withProperties(kafkaStringParams).withTopic(topic);

        final String keyGeneratorClazz = properties.getProperty("key.generator.class");

        if (keyGeneratorClazz != null) {
            final KafkaKeyGenerator<String, GenericRecord> kafkaKeyGenerator = KeyGeneratorUtil.getKeyGeneratorInstance(keyGeneratorClazz);
            kafkaGenericWriterBuilder.withKeyGenerator(kafkaKeyGenerator);
        }

        this.writer = kafkaGenericWriterBuilder.build();
    }

    /**
     * Send avro messages.
     *
     * @param dataset
     *            the data stream
     */
    @Override
    public void write(final Dataset dataset) {
        try {
            final Row first = (Row) dataset.head();

            if (first != null) {
                final String[] fieldNames = first.schema().fieldNames();
                final JavaRDD<Row> rdd = dataset.javaRDD();

                final JavaRDD<GenericRecord> transformRddToAvroRdd = transformRddToAvroRdd(rdd, schemaRegistryUrl, schemaRegistryCache, fieldNames,
                        getLookup());

                transformRddToAvroRdd.foreach(new VoidFunction<GenericRecord>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public void call(final GenericRecord record) {
                        writer.write(record);
                    }
                });
            }
        } catch (final GenericRecordConversionException | SchemaRetrievalException e) {
            LOGGER.error("BpsSparkKafkaDataSink Exception occured whilst sending dataframe, error ::", e);
        } finally {
            writer.flush();
        }
    }

    /**
     * Clean up.
     */
    @Override
    public void cleanUp() {
        if (writer != null) {
            writer.close();
        }
    }

    private RegisteredSchema getEventSchema(final Properties properties) {
        try {
            final SchemaRegistryClient schemaRegistryClient = initSchemaRegistryClient(schemaRegistryUrl, schemaRegistryCache);
            return schemaRegistryClient.lookup(properties.getProperty("eventType"));
        } catch (final SchemaRetrievalException e) {
            throw new IllegalArgumentException("Unable to retrieve event type:: " + properties.getProperty("eventType") + " from schema registry.",
                    e);
        }
    }

    private static SchemaRegistryClient initSchemaRegistryClient(final String schemaRegistryUrl, final int schemaRegistryCache) {
        final Properties properties = new Properties();
        properties.setProperty(SCHEMA_REGISTRY_ADDRESS_PARAMETER, schemaRegistryUrl);
        properties.setProperty(SCHEMA_REGISTRY_CACHE_MAX_SIZE_PARAMETER, String.valueOf(schemaRegistryCache));
        return SchemaRegistryClientFactory.newSchemaRegistryClientInstance(properties);
    }

    /**
     * if the getEventSchema call failed, we should handle this in a best effort manner.
     * @return the lookup
     */
    private RegisteredSchema getLookup() {
        if (lookup == null) {
            lookup = getEventSchema(super.getSinkProperties());
        }
        return lookup;
    }
}
