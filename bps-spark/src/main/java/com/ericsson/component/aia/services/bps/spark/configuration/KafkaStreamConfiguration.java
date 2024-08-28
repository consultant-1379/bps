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
package com.ericsson.component.aia.services.bps.spark.configuration;

import static com.ericsson.component.aia.services.bps.spark.common.KafkaConfigEnum.KEY_CLASS;
import static com.ericsson.component.aia.services.bps.spark.common.KafkaConfigEnum.KEY_DECODER_CLASS;
import static com.ericsson.component.aia.services.bps.spark.common.KafkaConfigEnum.KEY_SERIALIZER;
import static com.ericsson.component.aia.services.bps.spark.common.KafkaConfigEnum.VALUE_CLASS;
import static com.ericsson.component.aia.services.bps.spark.common.KafkaConfigEnum.VALUE_DECODER_CLASS;
import static com.ericsson.component.aia.services.bps.spark.common.KafkaConfigEnum.VALUE_SERIALIZER;
import static com.google.common.base.Preconditions.checkArgument;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections.map.HashedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.services.bps.core.common.Constants;
import com.ericsson.component.aia.services.bps.core.common.URIDefinition;
import com.ericsson.component.aia.services.bps.core.common.uri.IOURIS;
import com.ericsson.component.aia.services.bps.spark.common.KafkaVersionSpecficParams;
import com.ericsson.component.aia.services.bps.spark.common.avro.BpsKafkaGenericRecordDecoder;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;

import kafka.serializer.StringDecoder;

/**
 * The Class KafkaStreamConfiguration.
 */
public class KafkaStreamConfiguration implements Serializable {

    private static final String STRING_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 3064117827490530077L;

    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaStreamConfiguration.class);

    /** The brokers. */
    private String brokers;

    /** The topics set. */
    private final Set<String> topicsSet = new HashSet<String>();

    /** The kafka params. */
    private final Map<String, Object> kafkaParams = new HashMap<String, Object>();


    private String keySerializer;

    private String valueSerializer;

    /**
     * Instantiates a new kafka stream configuration.
     *
     * @param kafkaConfigs
     *            the configuration
     */
    public KafkaStreamConfiguration(final Properties kafkaConfigs) {
        final URIDefinition<IOURIS> decode = IOURIS.decode(kafkaConfigs);
        topicsSet.addAll(transformExtensionPoints(decode.getContext(), IOURIS.KAFKA.getUri()));
        brokers = null;
        initializeKafkaParams(kafkaConfigs);
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Configured parameters in flox xml {}", kafkaConfigs);
            LOGGER.info("Configuration passed to subscriber {}", this);
        }
    }

    /**
     * Initialize Kafka params.
     *
     * @param kafkaConfigs
     */
    @SuppressWarnings("PMD.UnusedFormalParameter")
    private void initializeKafkaParams(final Properties kafkaConfigs) {
        @SuppressWarnings("unchecked")
        final Map<String, String> params = new HashedMap(kafkaConfigs);
        checkArgument(
                (brokers = (String) kafkaConfigs.get(KafkaVersionSpecficParams.BROKER_LIST_08.getConfiguration())) != null
                        || (brokers = (String) kafkaConfigs.get(KafkaVersionSpecficParams.BROKER_LIST_09.getConfiguration())) != null,
                "Required broker property is missing provide either[ " + KafkaVersionSpecficParams.BROKER_LIST_08.getConfiguration() + " ] OR [ "
                        + KafkaVersionSpecficParams.BROKER_LIST_09.getConfiguration() + "]");
        checkArgument(
                !(kafkaConfigs.get(KafkaVersionSpecficParams.BROKER_LIST_08.getConfiguration()) != null
                        && kafkaConfigs.get(KafkaVersionSpecficParams.BROKER_LIST_09.getConfiguration()) != null),
                "Provided both [" + KafkaVersionSpecficParams.BROKER_LIST_08.getConfiguration() + "] and ["
                        + KafkaVersionSpecficParams.BROKER_LIST_09.getConfiguration() + "] but expected only one specific to kafka version");

        params.remove(Constants.URI);
        params.remove(com.ericsson.component.aia.services.bps.spark.common.Constants.WINDOW_LENGTH);
        params.remove(com.ericsson.component.aia.services.bps.spark.common.Constants.SLIDE_WINDOW_LENGTH);
        // input.name  is due to flow.xml this is handle as of now manually.
        params.remove("input.name");
        // setting  default values if not exists
        params.put(KEY_CLASS.getConfiguration(), kafkaConfigs.getProperty(KEY_CLASS.getConfiguration(), String.class.getCanonicalName()));
        params.put(VALUE_CLASS.getConfiguration(), kafkaConfigs.getProperty(VALUE_CLASS.getConfiguration(), GenericRecord.class.getCanonicalName()));
        params.put(KEY_DECODER_CLASS.getConfiguration(),
                kafkaConfigs.getProperty(KEY_DECODER_CLASS.getConfiguration(), StringDecoder.class.getCanonicalName()));
        params.put(VALUE_DECODER_CLASS.getConfiguration(),
                kafkaConfigs.getProperty(VALUE_DECODER_CLASS.getConfiguration(), BpsKafkaGenericRecordDecoder.class.getCanonicalName()));

        keySerializer = kafkaConfigs.getProperty(KEY_SERIALIZER.getConfiguration(), STRING_SERIALIZER);
        valueSerializer = kafkaConfigs.getProperty(VALUE_SERIALIZER.getConfiguration(), STRING_SERIALIZER);
        kafkaParams.putAll(params);
    }

    /**
     * Transform extension points.
     *
     * @param extensionPoint
     *            the e P
     * @param indexer
     *            the indexer
     * @return the collection
     */
    private Collection<String> transformExtensionPoints(final String extensionPoint, final String indexer) {
        final List<String> splitToList = Arrays.asList(extensionPoint.split(","));
        final Collection<String> transform = Collections2.transform(splitToList, new Function<String, String>() {
            @Override
            public String apply(final String arg0) {
                return (arg0.contains(indexer) ? arg0.replace(indexer, "") : arg0).trim();
            }
        });
        return transform;
    }

    /**
     * Gets the brokers.
     *
     * @return the brokers
     */
    public String getBrokers() {
        return brokers;
    }

    /**
     * Gets the kafka params.
     *
     * @return the kafkaParams
     */
    public Map<String, Object> getKafkaParams() {
        return kafkaParams;
    }

    /**
     * Gets the topics set.
     *
     * @return the topicsSet
     */
    public Set<String> getTopicsSet() {
        return topicsSet;
    }

    /**
     * @return the keySerializer
     */
    public String getKeySerializer() {
        return keySerializer;
    }

    /**
     * @return the valueSerializer
     */
    public String getValueSerializer() {
        return valueSerializer;
    }

}