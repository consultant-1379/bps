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
package com.ericsson.component.aia.services.bps.spark.datasinkservice;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.junit.Test;

import com.ericsson.component.aia.itpf.common.modeling.flow.schema.gen.fbp_flow.SinkType;
import com.ericsson.component.aia.services.bps.core.service.configuration.datasink.DefaultBpsDataSinkConfiguration;

/**
 * Test custom datawriter for KafkaSink.
 */
public class TestBpsSparkKafkaDataSink {

    /**
     * Valid DataWriter class
     */
    @SuppressWarnings("rawtypes")
    public static class ValidDataWriter extends DataWriter<Dataset> {

        private static final long serialVersionUID = 1L;

        @Override
        public void write(final Dataset data) {
        }

        @Override
        public void cleanUp() {
        }

    }

    /**
     * inValid DataWriter class
     */
    public static class InValidDataWriter {
    }

    /**
     * Test Sink initialized custom valid DataWriter properly or not.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    public void testDataWriterInitiliazedWithCustomWriter() {
        final BpsSparkKafkaDataSink sink = new BpsSparkKafkaDataSink();
        final Properties pro = commonProperties();
        pro.put("datawriter.class", ValidDataWriter.class.getName());

        final DefaultBpsDataSinkConfiguration uriOutput = new DefaultBpsDataSinkConfiguration();
        uriOutput.configure("TestCustomKafkaDataWriter", pro, Collections.<SinkType> emptyList());
        sink.configureDataSink(null, uriOutput);
        final Object object = getObject(sink);
        assertNotNull(object);
        assertEquals(true, object instanceof DataWriter);
        final DataWriter writer = (DataWriter) object;
        final Properties sinkProperties = writer.getSinkProperties();
        assertEquals(sinkProperties.get("uri"), pro.get("uri"));
        assertEquals(sinkProperties.get("bootstrap.servers"), pro.get("bootstrap.servers"));
        assertEquals(sinkProperties.get("eventType"), pro.get("eventType"));
        assertEquals(sinkProperties.get("valueClass"), pro.get("valueClass"));
        assertEquals(sinkProperties.get("value.serializer"), pro.get("value.serializer"));
        assertEquals(sinkProperties.get("schemaRegistry.address"), pro.get("schemaRegistry.address"));
        assertEquals(sinkProperties.get("datawriter.class"), pro.get("datawriter.class"));
    }

    /**
     * Test Sink expected invalid
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test(expected = IllegalArgumentException.class)
    public void testInvalidDataWriterInstance() {
        final BpsSparkKafkaDataSink sink = new BpsSparkKafkaDataSink();
        final Properties pro = commonProperties();
        pro.put("datawriter.class", InValidDataWriter.class.getName());
        final DefaultBpsDataSinkConfiguration uriOutput = new DefaultBpsDataSinkConfiguration();
        uriOutput.configure("TestCustomKafkaDataWriter", pro, Collections.<SinkType> emptyList());
        sink.configureDataSink(null, uriOutput);
    }

    /**
     * @return
     */
    private Properties commonProperties() {
        final Properties pro = new Properties();
        pro.put("uri", "kafka://calldrop_out_4?format=avro");
        pro.put("bootstrap.servers", "10.45.16.202:9092");
        pro.put("valueClass", "com.ericsson.component.aia.common.avro.kafka.encoder.KafkaGenericRecordEncoder");
        pro.put("value.serializer", "com.ericsson.component.aia.common.avro.kafka.encoder.KafkaGenericRecordEncoder");
        pro.put("schemaRegistry.address", "http://10.45.16.202:8081");
        return pro;
    }

    /**
     * Helper method to retrieve private field.
     *
     * @param sink
     * @return on success return field else null
     * @throws NoSuchFieldException
     * @throws IllegalAccessException
     */
    @SuppressWarnings("rawtypes")
    private Object getObject(final BpsSparkKafkaDataSink sink) {
        try {
            final Field declaredField = BpsSparkKafkaDataSink.class.getDeclaredField("dataWriter");
            declaredField.setAccessible(true);
            final Object object = declaredField.get(sink);
            declaredField.setAccessible(false);
            return object;
        } catch (final Exception e) {

        }
        return null;
    }
}
