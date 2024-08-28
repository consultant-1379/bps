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

import static com.ericsson.component.aia.services.bps.core.common.Constants.AVRO;
import static com.ericsson.component.aia.services.bps.core.common.Constants.BOOTSTRAP_SERVERS;
import static com.ericsson.component.aia.services.bps.core.common.Constants.EVENT_TYPE;
import static com.ericsson.component.aia.services.bps.core.common.Constants.FORMAT;
import static com.ericsson.component.aia.services.bps.core.common.Constants.JSON;
import static com.ericsson.component.aia.services.bps.core.common.Constants.SERIALIZATION_SCHEMA;

import java.util.Properties;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.model.registry.exception.SchemaRetrievalException;
import com.ericsson.component.aia.model.registry.impl.RegisteredSchema;
import com.ericsson.component.aia.services.bps.core.common.URIDefinition;
import com.ericsson.component.aia.services.bps.core.common.uri.IOURIS;
import com.ericsson.component.aia.services.bps.core.exception.BpsRuntimeException;
import com.ericsson.component.aia.services.bps.core.exception.GenericRecordConversionException;
import com.ericsson.component.aia.services.bps.core.service.configuration.datasink.BpsDataSinkConfiguration;
import com.ericsson.component.aia.services.bps.core.service.streams.BpsAbstractDataSink;
import com.ericsson.component.aia.services.bps.flink.kafka.encoder.DataStreamToAvro;
import com.ericsson.component.aia.services.bps.flink.utils.SchemaRegistryClientUtil;

/**
 * The <code>BpsFlinkKafkaDataSinkService</code> is responsible for producing Kafka events (Kafka producer) from # {@link DataStream}.
 *
 * The <code>BpsFlinkKafkaDataSinkService</code> implements <code>BpsDataSinkService&lt;StreamExecutionEnvironment,
 * DataStream&gt;</code> which is specific to StreamExecutionEnvironment and DataStream.
 *
 * @param <T>
 */
public class BpsFlinkKafkaDataSinkService<T> extends BpsAbstractDataSink<StreamExecutionEnvironment, DataStream<T>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BpsFlinkKafkaDataSinkService.class);

    private String eventType;

    private String kafkaBrokers;

    private String topic;

    private String kafkaSerializationSchemaClassName;

    private String dataFormat;

    private RegisteredSchema registeredSchema;

    /**
     * Returns the service context name.
     */
    @Override
    public String getServiceContextName() {
        LOGGER.trace("Service context name={}", IOURIS.KAFKA.getUri());
        return IOURIS.KAFKA.getUri();
    }

    /**
     * Configured instance of {@link BpsFlinkKafkaDataSinkService} for the specified sinkContextName.<br>
     * sinkContextName is the name of the output sink which was configured in flow.xml through tag &lt;output name="sinkContextName"&gt; ....
     * &lt;/output&gt;
     *
     * @param context
     *            the context for which Sink needs to be configured.
     * @param bpsDataSinkConfiguration
     *            the bps data sink configuration
     */
    @Override
    public void configureDataSink(final StreamExecutionEnvironment context, final BpsDataSinkConfiguration bpsDataSinkConfiguration) {
        super.configureDataSink(context, bpsDataSinkConfiguration);
        LOGGER.trace("Initiating configureDataSink for {}. ", getDataSinkContextName());
        LOGGER.info("Configuring {} for the output ", this.getClass().getName(), bpsDataSinkConfiguration.getDataSinkContextName());
        final URIDefinition<IOURIS> decode = IOURIS.decode(properties);
        final Properties params = decode.getParams();
        dataFormat = params.getProperty(FORMAT);
        if (!AVRO.equalsIgnoreCase(dataFormat) && !JSON.equalsIgnoreCase(dataFormat)) {
            throw new BpsRuntimeException("Currenlty Kafka supports avro & json formats only");
        }
        eventType = properties.getProperty(EVENT_TYPE);
        kafkaBrokers = properties.getProperty(BOOTSTRAP_SERVERS);
        topic = decode.getContext();
        kafkaSerializationSchemaClassName = properties.getProperty(SERIALIZATION_SCHEMA);
        if (AVRO.equalsIgnoreCase(dataFormat)) {
            try {
                registeredSchema = SchemaRegistryClientUtil.getSchemaRegistryClient(properties).lookup(eventType);
            } catch (final SchemaRetrievalException e) {
                LOGGER.error("Got exception while trying to lookup event type {} and exception is ", eventType, e);
            }
        } else {
            LOGGER.info("BpsSparkKafkaDataSink.configureDataSink -> Output Data Format is {} . Will not look up Schema Registry for schemas",
                    dataFormat);
        }
        LOGGER.trace("Finished configureDataSink method for the sink context name {} ", bpsDataSinkConfiguration.getDataSinkContextName());
    }

    @SuppressWarnings("unchecked")
    @Override
    public void write(final DataStream<T> dataStream) {
        LOGGER.info("BpsFlinkKafkaDataSinkService [Context=" + getDataSinkContextName() + "] got DataStream");

        DataStream<T> sinkStream = dataStream;
        if (AVRO.equalsIgnoreCase(dataFormat)) {
            try {
                sinkStream = (DataStream<T>) DataStreamToAvro.transformAvroDataStream(dataStream, registeredSchema);
            } catch (final GenericRecordConversionException exp) {
                LOGGER.error("BpsFlinkKafkaDataSinkService [Context=" + getDataSinkContextName() + "] got error while processing dataStream ", exp);
            }
        }

        addFlinkKafkaProducer(sinkStream);
        LOGGER.info("BpsFlinkKafkaDataSinkService [Context=" + getDataSinkContextName() + "] write operation completed successfuly.");
    }

    @Override
    public void cleanUp() {

    }

    @SuppressWarnings("unchecked")
    private SerializationSchema<T> getKafkaSerializer(final String kafkaSerializationSchemaClassName) {
        try {
            final ClassLoader classLoader = BpsFlinkKafkaDataSinkService.class.getClassLoader();
            final SerializationSchema<T> KafkaDeserializar = (SerializationSchema<T>) classLoader.loadClass(kafkaSerializationSchemaClassName)
                    .newInstance();
            return KafkaDeserializar;
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException exp) {
            throw new IllegalArgumentException(String.format("Unable to create class with name=%s", kafkaSerializationSchemaClassName), exp);
        }
    }

    private void addFlinkKafkaProducer(final DataStream<T> sinkStream) {
        sinkStream.addSink(new FlinkKafkaProducer010<>(kafkaBrokers, topic, getKafkaSerializer(kafkaSerializationSchemaClassName)));
    }
}
