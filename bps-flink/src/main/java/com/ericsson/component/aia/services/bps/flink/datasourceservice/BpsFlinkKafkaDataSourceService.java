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
package com.ericsson.component.aia.services.bps.flink.datasourceservice;

import static com.ericsson.component.aia.services.bps.core.common.Constants.BOOTSTRAP_SERVERS;
import static com.ericsson.component.aia.services.bps.core.common.Constants.DESERIALIZATION_SCHEMA;
import static com.ericsson.component.aia.services.bps.core.common.Constants.GROUP_ID;

import java.util.Map;
import java.util.Properties;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.services.bps.core.common.URIDefinition;
import com.ericsson.component.aia.services.bps.core.common.uri.IOURIS;
import com.ericsson.component.aia.services.bps.core.datasourceservice.AbstractBpsDataSourceService;
import com.ericsson.component.aia.services.bps.flink.kafka.decoder.FlinkDeserializationSchema;
import com.ericsson.component.aia.services.bps.flink.utils.SchemaRegistryClientUtil;

/**
 * The <code>BpsFlinkKafkaDataSourceService</code> is responsible for reading data from Kafka (Kafka consumer) and return respectiveb#
 * {@link DataStream}.
 *
 * The <code>BpsFlinkKafkaDataSourceService</code> implements <code>BpsDataSourceService&lt;StreamExecutionEnvironment,
 * DataStream&gt;</code> which is specific to StreamExecutionEnvironment and DataStream.
 */
public class BpsFlinkKafkaDataSourceService extends AbstractBpsDataSourceService<StreamExecutionEnvironment, DataStream<?>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BpsFlinkKafkaDataSourceService.class);

    /**
     * Returns the service context name.
     */
    @Override
    public String getServiceContextName() {
        LOGGER.trace("Service context name={}", IOURIS.KAFKA.getUri());
        return IOURIS.KAFKA.getUri();
    }

    /**
     * Retrieves the data stream.
     */
    @Override
    public DataStream<?> getDataStream() {
        final URIDefinition<IOURIS> decode = IOURIS.decode(properties);
        final String kafkaBrokers = properties.getProperty(BOOTSTRAP_SERVERS);
        final String consumerGroupId = properties.getProperty(GROUP_ID);
        final String topic = decode.getContext();
        final String kafkaDeserializationSchemaClassName = properties.getProperty(DESERIALIZATION_SCHEMA);
        LOGGER.debug(
                "Creating data stream for kafka with brokers={},topic={},consumerGroupId={},deserialization schema={}",
                kafkaBrokers, topic, consumerGroupId, kafkaDeserializationSchemaClassName);
        final DeserializationSchema<?> KafkaDeserializar = getKafkaDeserializer(kafkaDeserializationSchemaClassName);
        final DataStream<?> stream = context.addSource(getFlinkKafkaConsumer(topic, KafkaDeserializar));
        LOGGER.debug("DataStream creation successful for kafka with version={},brokers={},topic={},groupId={},deserializationSchema={}",
                kafkaBrokers, topic, consumerGroupId, kafkaDeserializationSchemaClassName);
        return stream;
    }

    private DeserializationSchema<?> getKafkaDeserializer(final String kafkaDeserializationSchemaClassName) {
        try {
            final DeserializationSchema<?> KafkaDeserializar = (DeserializationSchema<?>) BpsFlinkKafkaDataSourceService.class
                    .getClassLoader().loadClass(kafkaDeserializationSchemaClassName).newInstance();
            if (KafkaDeserializar instanceof FlinkDeserializationSchema) {
                updateSchemaRegistryClientProperties((FlinkDeserializationSchema<?>) KafkaDeserializar);
            }
            return KafkaDeserializar;
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException exp) {
            throw new IllegalArgumentException(String.format("Unable to create class with name=%s", kafkaDeserializationSchemaClassName), exp);
        }
    }

    private void updateSchemaRegistryClientProperties(final FlinkDeserializationSchema<?> KafkaDeserializar) {
        final Map<String, String> schemaRegistryClientProperties = SchemaRegistryClientUtil
                .getSchemaRegistryClientProperties(properties);
        if (!schemaRegistryClientProperties.isEmpty()) {
            final Properties KafkaDeserializarProperties = new Properties();
            KafkaDeserializarProperties.putAll(schemaRegistryClientProperties);
            KafkaDeserializar.setProperties(KafkaDeserializarProperties);
        }
    }

    private FlinkKafkaConsumerBase<?> getFlinkKafkaConsumer(final String topic, final DeserializationSchema<?> KafkaDeserializar) {
        return new FlinkKafkaConsumer010<>(topic, KafkaDeserializar, properties);
    }
}
