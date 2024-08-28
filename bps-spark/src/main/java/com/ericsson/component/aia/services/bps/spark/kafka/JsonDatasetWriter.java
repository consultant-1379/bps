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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;

import com.ericsson.component.aia.common.transport.kafka.writer.builder.KafkaGenericWriterBuilder;
import com.ericsson.component.aia.common.transport.kafka.writer.impl.KafkaGenericWriter;
import com.ericsson.component.aia.services.bps.spark.configuration.KafkaStreamConfiguration;
import com.ericsson.component.aia.services.bps.spark.datasinkservice.DataWriter;

/**
 * The Class JsonDataFrameWriter is used to write data frames to KAFKA as JSON.
 */
public class JsonDatasetWriter extends DataWriter<Dataset> {

    /** serialVersionUID. */
    private static final long serialVersionUID = -4916428491473581453L;

    /** The writer. */
    private final KafkaGenericWriter<String, String> writer;

    /**
     * Instantiates a new json data frame writer.
     *
     * @param properties
     *            the properties
     * @param topic
     *            the topic
     */
    public JsonDatasetWriter(final Properties properties, final String topic) {
        super.setSinkProperties(properties);
        final KafkaStreamConfiguration kafkaStreamConfiguration = new KafkaStreamConfiguration(properties);
        final Map<String, Object> kafkaParams = kafkaStreamConfiguration.getKafkaParams();

        final Map<String, String> kafkaStringParams = new HashMap<String, String>();
        for (final String key : kafkaParams.keySet()) {
            kafkaStringParams.put(key, (String) kafkaParams.get(key));
        }

        final KafkaGenericWriter<String, String> writer = KafkaGenericWriterBuilder.<String, String> create()
                .withBrokers(kafkaStreamConfiguration.getBrokers()).withKeySerializer(kafkaStreamConfiguration.getKeySerializer())
                .withValueSerializer(kafkaStreamConfiguration.getValueSerializer()).withProperties(kafkaStringParams).withTopic(topic).build();

        this.writer = writer;
    }

    /**
     * Write data frame to KAFKA as JSON.
     *
     * @param dataFrame
     *            the data frame
     */
    @Override
    public void write(final Dataset dataFrame) {

        final JavaRDD<String> jsonRDD = dataFrame.toJSON().toJavaRDD();
        jsonRDD.foreach(new VoidFunction<String>() {

            private static final long serialVersionUID = 2190818613266986028L;

            @Override
            public void call(final String json) {
                writer.write(json);
            }
        });
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
}
