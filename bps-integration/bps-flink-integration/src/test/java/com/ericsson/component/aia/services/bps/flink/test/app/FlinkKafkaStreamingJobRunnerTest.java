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
package com.ericsson.component.aia.services.bps.flink.test.app;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.model.registry.exception.SchemaRetrievalException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Integration test class to test SampleFlinkStreamingJobRunner class for Kafka Flink Streaming test cases.
 */
public class FlinkKafkaStreamingJobRunnerTest extends AbstractFlinkStreamingJobRunnerTestBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkKafkaStreamingJobRunnerTest.class);

    private static final String EXPECTED_OUTPUT_FILE_PATH = "src/test/resources/ExpectedTextOutput";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private Properties kafkaInputProperties;

    private Properties fileOutputProperties;

    private Properties kafkaOutputProperties;

    private Properties stepProperties;

    private String flinkOutputDataDir;

    @BeforeClass
    public static void setUpKafka() throws Exception {
        initKafka();
    }

    @Test
    public void testKafkaToFileSink_AVRO() throws InterruptedException, IOException, SchemaRetrievalException {
        configureKafkaInputProperties("com.ericsson.component.aia.services.bps.flink.kafka.decoder.FlinkKafkaGenericRecordDeserializationSchema",
                "avro", KAFKA_AVRO_TOPIC);
        configureStepProperties("com.ericsson.component.aia.services.bps.flink.test.app.SampleFlinkStreamingJobRunner");
        configureFileOutputProperties();
        final TestKafkaToFileExecutor testAppExecutor = new TestKafkaToFileExecutor();
        testAppExecutor.start();
        LOGGER.info("**************Sleeping for 10 seconds for starting flink test app**************************");
        testAppExecutor.join(10000);
        produceMessage(TestDataFormat.AVRO);
        LOGGER.info("********Sleeping for 5 seconds**********");
        Thread.sleep(5000);
        validateFileOutput();
    }

    @Test
    public void testKafkaToKafkaSink_JSON() throws InterruptedException, IOException, SchemaRetrievalException {
        final String deSerializationSchemaClass = "com.ericsson.component.aia.services.bps.flink.test.app.FlinkKafkaJsonFlinkDeserializationSchema";
        runKafkaToKafka(deSerializationSchemaClass);
    }

    @Test
    public void test_runs_from_kafka_to_kafka_JSON_using_flink_deserialization_schema() throws InterruptedException, IOException, SchemaRetrievalException {
        final String deSerializationSchemaClass = "com.ericsson.component.aia.services.bps.flink.test.app.FlinkKafkaJsonDeserializationSchema";
        runKafkaToKafka(deSerializationSchemaClass);
    }

    private void runKafkaToKafka(final String deSerializationSchemaClass) throws InterruptedException, JsonProcessingException, SchemaRetrievalException {
        configureKafkaInputProperties(deSerializationSchemaClass, "json", KAFKA_JSON_TOPIC);
        configureStepProperties("com.ericsson.component.aia.services.bps.flink.test.app.SampleFlinkStreamingJsonJobRunner");
        configureKafkaOutputProperties("com.ericsson.component.aia.services.bps.flink.test.app.FlinkKafkaJsonSerializationSchema", "json");
        final ExtendedKafkaOutTopicSubscriber subscriber = new ExtendedKafkaOutTopicSubscriber();
        final Thread subscriberThread = new Thread(subscriber);
        subscriberThread.start();
        final TestKafkaToKafkaExecutor testAppExecutor = new TestKafkaToKafkaExecutor();
        testAppExecutor.start();
        LOGGER.info("**************Sleeping for 10 seconds for starting flink test app**************************");
        testAppExecutor.join(10000);
        produceMessage(TestDataFormat.JSON);
        LOGGER.info("********Sleeping for 5 seconds**********");
        subscriberThread.join(600000);
        final List<Object> receivedMsg = (List<Object>) subscriber.getMessages();
        validateKafkaJsonOutput(receivedMsg);
    }

    private void validateKafkaJsonOutput(final List<Object> receivedMsg) {
        assertEquals(1, receivedMsg.size());
        final String jsonMsg = (String) receivedMsg.get(0);
        PositionEvent event = new PositionEvent();
        try {
            event = MAPPER.readValue(jsonMsg, PositionEvent.class);
        } catch (final IOException e) {
            fail("Could not decode JSON output..." + e);
        }
        assertEquals("123", event.getVin());
        assertEquals(12.3, event.getBearing(), 0);
        assertEquals(53.4, event.getLatitidue(), 0);
        assertEquals(12.4, event.getLongitude(), 0);
        //position time won't be checked
    }

    private void validateFileOutput() throws IOException {
        final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        final String dateAsString = format.format(Calendar.getInstance().getTime());
        final String actualOutputDataFilePath = flinkOutputDataDir + File.separator + dateAsString;
        assertEquals("The files differ!", FileUtils.readLines(new File(actualOutputDataFilePath), "utf-8").get(0),
                FileUtils.readLines(new File(EXPECTED_OUTPUT_FILE_PATH), "utf-8").get(0));
    }

    private void configureStepProperties(final String driverClassName) {
        stepProperties = new Properties();
        stepProperties.put("uri", "flink-streaming://EpsFlinkStreamingHandler");
        stepProperties.put("driver-class", driverClassName);
    }

    private void configureFileOutputProperties() {
        flinkOutputDataDir = kafkaTempDir.toFile().getAbsolutePath() + File.separator + "flink";
        fileOutputProperties = new Properties();
        fileOutputProperties.put("uri", getFileUri() + flinkOutputDataDir);
        fileOutputProperties.put("data.format", "txt");
    }

    private void configureKafkaInputProperties(final String deSerializationSchemaClass, final String dataFormat, final String topicName) {
        kafkaInputProperties = kafkaInputProperties(deSerializationSchemaClass, dataFormat, topicName);
        kafkaInputProperties.put("schemaRegistry.address", SCHEMA_DIR);
        kafkaInputProperties.put("schemaRegistry.cacheMaximumSize", "50");
    }

    private void configureKafkaOutputProperties(final String serializationSchemaClass, final String dataFormat) {
        kafkaOutputProperties = new Properties();
        kafkaOutputProperties.put("uri", "kafka://radio_session_topic_out?format=" + dataFormat);
        kafkaOutputProperties.put("bootstrap.servers", LOCAL_HOST + ":" + kafkaPort);
        kafkaOutputProperties.put("version", "10");
        kafkaOutputProperties.put("eventType", "pojo.POJO");
        if (null != serializationSchemaClass) {
            kafkaOutputProperties.put("serialization.schema", serializationSchemaClass);
        }
    }

    public class TestKafkaToFileExecutor extends Thread {

        @Override
        public void run() {
            LOGGER.info("**************Starting pipeline KAFKA -> Flink -> FILE **************************");
            final Map<String, Map<String, String>> context = new HashMap<String, Map<String, String>>();
            context.put("attributeIPMap", (Map) kafkaInputProperties);
            context.put("attributeOPMap", (Map) fileOutputProperties);
            context.put("attributeStepMap", (Map) stepProperties);
            executePipeLine(context, kafkaTempDir, "KAFKA-To-TXT");
        }
    }

    public class TestKafkaToKafkaExecutor extends Thread {

        @Override
        public void run() {
            LOGGER.info("**************Starting pipeline KAFKA -> Flink -> KAFKA **************************");
            final Map<String, Map<String, String>> context = new HashMap<String, Map<String, String>>();
            context.put("attributeIPMap", (Map) kafkaInputProperties);
            context.put("attributeOPMap", (Map) kafkaOutputProperties);
            context.put("attributeStepMap", (Map) stepProperties);
            executePipeLine(context, kafkaTempDir, "KAFKA-To-KAFKA");
        }

    }

    @AfterClass
    public static void trearDown() throws Exception {
        tearDownKafka();
    }

}
