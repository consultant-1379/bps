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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.junit.Test;

public class KafkaStreamConfigurationTest {

    @Test
    public void testKafkaVersion08ConfigurationSettings() {

        final Properties kafkaProps = getKafkaProperties(8);
        final KafkaStreamConfiguration kafkaConfigs = new KafkaStreamConfiguration(kafkaProps);
        final Set<String> topicsSet = new HashSet<String>();
        topicsSet.add("test");
        assertEquals(topicsSet, kafkaConfigs.getTopicsSet());
        assertEquals(9, kafkaConfigs.getKafkaParams().size());
        assertEquals("3000", kafkaConfigs.getKafkaParams().get("rebalance.max.retries"));
        assertEquals("5000", kafkaConfigs.getKafkaParams().get("auto.commit.interval.ms"));
        assertEquals("earliest", kafkaConfigs.getKafkaParams().get("auto.offset.reset"));
        assertEquals("testgroup", kafkaConfigs.getKafkaParams().get("group.id"));
        assertEquals("testingkafka08:90922,testingkafka08:9092", kafkaConfigs.getKafkaParams().get("metadata.broker.list"));
        assertEquals("keyDecoder", kafkaConfigs.getKafkaParams().get("keyDecoder.class"));
        assertEquals("keyClass", kafkaConfigs.getKafkaParams().get("keyClass"));
        assertEquals("valueClass", kafkaConfigs.getKafkaParams().get("valueClass"));
        assertEquals("valueDecoder", kafkaConfigs.getKafkaParams().get("valueDecoder.class"));

    }

    @Test
    public void testKafkaVersion09ConfigurationSettings() {
        final Properties kafkaProps = getKafkaProperties(9);
        final KafkaStreamConfiguration kafkaConfigs = new KafkaStreamConfiguration(kafkaProps);
        final Set<String> topicsSet = new HashSet<String>();
        topicsSet.add("ctr");
        topicsSet.add("ctum");
        topicsSet.add("calldrop");
        assertEquals(topicsSet, kafkaConfigs.getTopicsSet());
        assertEquals(9, kafkaConfigs.getKafkaParams().size());
        assertEquals("3000", kafkaConfigs.getKafkaParams().get("rebalance.max.retries"));
        assertEquals("5000", kafkaConfigs.getKafkaParams().get("auto.commit.interval.ms"));
        assertEquals("earliest", kafkaConfigs.getKafkaParams().get("auto.offset.reset"));
        assertEquals("testgroup", kafkaConfigs.getKafkaParams().get("group.id"));
        assertEquals("testingkafka09:90922,testingkafka09:9092", kafkaConfigs.getKafkaParams().get("bootstrap.servers"));
        assertEquals("keyDecoder", kafkaConfigs.getKafkaParams().get("keyDecoder.class"));
        assertEquals("keyClass", kafkaConfigs.getKafkaParams().get("keyClass"));
        assertEquals("valueClass", kafkaConfigs.getKafkaParams().get("valueClass"));
        assertEquals("valueDecoder", kafkaConfigs.getKafkaParams().get("valueDecoder.class"));
    }

    @Test
    public void testFlowPropertyInputNameIsNotExistInKafkaParams() {
        final Properties kafkaProps = getKafkaProperties(9);
        kafkaProps.put("input.name", "radio-input");
        final KafkaStreamConfiguration kafkaConfigs = new KafkaStreamConfiguration(kafkaProps);
        assertEquals(9, kafkaConfigs.getKafkaParams().size());
        assertEquals("3000", kafkaConfigs.getKafkaParams().get("rebalance.max.retries"));
        assertEquals("5000", kafkaConfigs.getKafkaParams().get("auto.commit.interval.ms"));
        assertEquals("earliest", kafkaConfigs.getKafkaParams().get("auto.offset.reset"));
        assertEquals("testgroup", kafkaConfigs.getKafkaParams().get("group.id"));
        assertEquals("testingkafka09:90922,testingkafka09:9092", kafkaConfigs.getKafkaParams().get("bootstrap.servers"));
        assertEquals("keyDecoder", kafkaConfigs.getKafkaParams().get("keyDecoder.class"));
        assertEquals("keyClass", kafkaConfigs.getKafkaParams().get("keyClass"));
        assertEquals("valueClass", kafkaConfigs.getKafkaParams().get("valueClass"));
        assertEquals("valueDecoder", kafkaConfigs.getKafkaParams().get("valueDecoder.class"));
        assertNull(kafkaConfigs.getKafkaParams().get("input.name"));

    }

    @Test(expected = java.lang.IllegalArgumentException.class)
    public void testBrokerSpecificToKafka08and09BothExistsThenThrowCheckException() {
        final Properties kafkaProps = getKafkaProperties(9);
        kafkaProps.setProperty("metadata.broker.list", "testingkafka08:90922,testingkafka08:9092");
        final KafkaStreamConfiguration kafkaConfigs = new KafkaStreamConfiguration(kafkaProps);
    }

    @Test(expected = java.lang.IllegalArgumentException.class)
    public void testMissingBrokerDetails() {
        final Properties properties = new Properties();
        properties.setProperty("uri", "kafka://test");
        new KafkaStreamConfiguration(properties);
    }

    public static Properties getKafkaProperties(final int version) {
        final Properties properties = new Properties();

        if (version == 8) {
            properties.setProperty("uri", "kafka://test");
            properties.setProperty("metadata.broker.list", "testingkafka08:90922,testingkafka08:9092");
        } else if (version == 9) {
            properties.setProperty("uri", "kafka://ctr,ctum,calldrop?format=avro");
            properties.setProperty("bootstrap.servers", "testingkafka09:90922,testingkafka09:9092");
        }

        properties.setProperty("slide.window.length", "10");
        properties.setProperty("window.length", "100");
        properties.setProperty("valueDecoder.class", "valueDecoder");
        properties.setProperty("keyDecoder.class", "keyDecoder");
        properties.setProperty("valueClass", "valueClass");
        properties.setProperty("keyClass", "keyClass");

        properties.setProperty("group.id", "testgroup");
        properties.setProperty("rebalance.max.retries", "3000");
        properties.setProperty("auto.commit.interval.ms", "5000");
        properties.setProperty("auto.offset.reset", "earliest");

        return properties;
    }
}
