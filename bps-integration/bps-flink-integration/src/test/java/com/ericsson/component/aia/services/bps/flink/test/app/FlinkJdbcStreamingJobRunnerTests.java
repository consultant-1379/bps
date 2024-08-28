package com.ericsson.component.aia.services.bps.flink.test.app;

import static com.ericsson.component.aia.services.bps.core.common.Constants.SEPARATOR;
import static com.ericsson.component.aia.services.bps.flink.common.Constants.AVRO_JDBC_TABLE_NAME;
import static com.ericsson.component.aia.services.bps.flink.common.Constants.EXPECTED_AVRO_DATA_SET;
import static com.ericsson.component.aia.services.bps.flink.common.Constants.EXPECTED_CSV_DATA_SET;
import static com.ericsson.component.aia.services.bps.flink.common.Constants.EXPECTED_JSON_DATA_SET;
import static com.ericsson.component.aia.services.bps.flink.common.Constants.JDBC_INPUT_DATASET;
import static com.ericsson.component.aia.services.bps.flink.common.Constants.JDBC_INPUT_EVENT_DATASET;
import static com.ericsson.component.aia.services.bps.flink.common.Constants.JSON_JDBC_TABLE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.FileDeleteStrategy;
import org.apache.commons.io.FileUtils;
import org.h2.Driver;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.model.registry.exception.SchemaRetrievalException;
import com.ericsson.component.aia.services.bps.core.common.uri.IOURIS;
import com.ericsson.component.aia.services.bps.flink.common.Constants;

/**
 * Integration test class to test SampleFlinkStreamingJobRunner class for Kafka to jdbc Flink Streaming test cases.
 */
@SuppressWarnings("rawtypes")
public class FlinkJdbcStreamingJobRunnerTests extends AbstractFlinkStreamingJobRunnerTestBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkJdbcStreamingJobRunnerTests.class);

    private Properties jdbcInputProperties;

    private Properties jdbcOutputProperties;

    private Properties stepProperties;

    private Properties kafkaInputProperties;

    private Properties kafkaOutputProperties;

    @BeforeClass
    public static void setUpKafka() throws Exception {
        initKafka();
    }

    @AfterClass
    public static void trearDown() throws Exception {
        tearDownKafka();

    }

    private void prepareTestDataBase(final String csvFile) {
        final String dir = Constants.ROOT_BASE_FOLDER + FlinkJdbcStreamingJobRunnerTests.class.getCanonicalName() + System.currentTimeMillis();
        final File fileDir = new File(dir);

        if (fileDir.exists()) {
            fileDir.delete();
        }
        fileDir.mkdir();
        jdbcTmpDir = fileDir.toPath();
        DB_LOCATION = "jdbc:h2:" + jdbcTmpDir.toAbsolutePath() + File.separator + "H2_JDBC";
        try {
            Class.forName(Driver.class.getCanonicalName());
            connection = DriverManager.getConnection(DB_LOCATION, "test", "mine");
            statement = connection.createStatement();
            statement.executeUpdate("DROP TABLE IF EXISTS " + INPUT_TABLE + "");
            if (csvFile.equalsIgnoreCase(JDBC_INPUT_EVENT_DATASET)) {
                statement.executeUpdate("CREATE TABLE " + INPUT_TABLE + "(SCANNER_ID BIGINT, RBSMODULE_ID BIGINT) AS SELECT * FROM CSVREAD('"
                        + JDBC_INPUT_EVENT_DATASET + "');");
            } else {
                statement.executeUpdate("CREATE TABLE " + INPUT_TABLE + " AS SELECT * FROM CSVREAD('" + JDBC_INPUT_DATASET + "');");
            }
            LOGGER.debug("Table created");
            LOGGER.debug("Connection created succesfully");
        } catch (final Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    private void configureInputProperties() {
        jdbcInputProperties = new Properties();
        jdbcInputProperties.put("uri", IOURIS.JDBC.getUri() + DB_LOCATION);
        jdbcInputProperties.put("driver", Driver.class.getCanonicalName());
        jdbcInputProperties.put("user", user);
        jdbcInputProperties.put("password", password);
        jdbcInputProperties.put("table.name", INPUT_TABLE);
    }

    private void configureOutputProperties() {
        jdbcOutputProperties = new Properties();
        jdbcOutputProperties.put("driver", Driver.class.getCanonicalName());
        jdbcOutputProperties.put("user", user);
        jdbcOutputProperties.put("password", password);
        jdbcOutputProperties.put("data.save.mode", "Overwrite");
    }

    private void configureKafkaOutputProperties(final String serializationSchemaClass, final String dataFormat) {
        kafkaOutputProperties = new Properties();
        kafkaOutputProperties.put("uri", "kafka://radio_session_topic_out?format=" + dataFormat);
        kafkaOutputProperties.put("bootstrap.servers", LOCAL_HOST + ":" + kafkaPort);
        kafkaOutputProperties.put("version", "10");
        kafkaOutputProperties.put("eventType", "tablepojo.TablePojo");
        if (null != serializationSchemaClass) {
            kafkaOutputProperties.put("serialization.schema", serializationSchemaClass);
        }
    }

    private void configureStepProperties() {
        stepProperties = new Properties();
        stepProperties.put("uri", "flink-streaming://EpsFlinkStreamingHandler");
        stepProperties.put("driver-class", "com.ericsson.component.aia.services.bps.flink.test.app.EpsFlinkStreamingHandler");
    }

    private void configureKafkaInputProperties(final String deSerializationSchemaClass, final String dataFormat, final String topicName) {
        kafkaInputProperties = kafkaInputProperties(deSerializationSchemaClass, dataFormat, topicName);
    }

    @Test
    public void testKafkaToJdbcSink_AVRO() throws InterruptedException, IOException, SQLException, ClassNotFoundException, SchemaRetrievalException {
        configureKafkaInputProperties("com.ericsson.component.aia.services.bps.flink.kafka.decoder.FlinkKafkaGenericRecordDeserializationSchema",
                "avro", KAFKA_AVRO_TOPIC);
        configureOutputProperties();
        jdbcOutputProperties.put("uri", IOURIS.JDBC.getUri() + "jdbc:h2:" + kafkaTempDir.toAbsolutePath() + File.separator + "H2_JDBC");
        jdbcOutputProperties.put("output.schema", "com.ericsson.component.aia.services.bps.flink.test.app.POJO");
        jdbcOutputProperties.put("table.name", "EVENTS_OUTPUT");

        final TestKafkaToJdbcExecutor testAppExecutor = new TestKafkaToJdbcExecutor();
        testAppExecutor.start();
        LOGGER.info("**************Sleeping for 10 seconds for starting flink test app**************************");
        testAppExecutor.join(10000);
        produceMessage(TestDataFormat.AVRO);
        LOGGER.info("********Sleeping for 5 seconds**********");
        Thread.sleep(5000);
        validate(EXPECTED_AVRO_DATA_SET, AVRO_JDBC_TABLE_NAME);
    }

    @Test
    public void testKafkaToJdbcSink_JSON() throws InterruptedException, IOException, SQLException, ClassNotFoundException, SchemaRetrievalException {
        configureKafkaInputProperties("com.ericsson.component.aia.services.bps.flink.test.app.FlinkKafkaJsonFlinkDeserializationSchema", "json",
                KAFKA_JSON_TOPIC);
        configureOutputProperties();
        jdbcOutputProperties.put("uri", IOURIS.JDBC.getUri() + "jdbc:h2:" + kafkaTempDir.toAbsolutePath() + File.separator + "H2_JDBC");
        jdbcOutputProperties.put("output.schema", "com.ericsson.component.aia.services.bps.flink.test.app.PositionEvent");
        jdbcOutputProperties.put("table.name", "POSITION_OUTPUT");

        final ExtendedKafkaOutTopicSubscriber subscriber = new ExtendedKafkaOutTopicSubscriber();
        final Thread subscriberThread = new Thread(subscriber);
        subscriberThread.start();
        final TestKafkaToJdbcExecutor testAppExecutor = new TestKafkaToJdbcExecutor();
        testAppExecutor.start();
        LOGGER.info("**************Sleeping for 10 seconds for starting flink test app**************************");
        testAppExecutor.join(10000);
        produceMessage(TestDataFormat.JSON);
        LOGGER.info("********Sleeping for 5 seconds**********");
        subscriberThread.join(50000);
        validate(EXPECTED_JSON_DATA_SET, JSON_JDBC_TABLE_NAME);
    }

    @Test
    public void testJdbcToKafkaSink_Json() throws Exception {
        prepareTestDataBase(JDBC_INPUT_EVENT_DATASET);
        configureInputProperties();
        configureKafkaOutputProperties("com.ericsson.component.aia.services.bps.flink.test.app.FlinkKafkaJsonRowSerializationSchema", "json");
        final ExtendedKafkaOutTopicSubscriber subscriber = new ExtendedKafkaOutTopicSubscriber();
        final Thread subscriberThread = new Thread(subscriber);
        subscriberThread.start();
        final TestJdbcToKafkaExecutor testAppExecutor = new TestJdbcToKafkaExecutor();
        testAppExecutor.start();
        LOGGER.info("**************Sleeping for 10 seconds for starting flink test app**************************");
        testAppExecutor.join(10000);
        LOGGER.info("********Sleeping for 5 seconds**********");
        subscriberThread.join(60000);
        final List<Object> receivedMsg = (List<Object>) subscriber.getMessages();
        validateKafkaAJsonOutput(receivedMsg);

    }

    private void validateKafkaAJsonOutput(final List<Object> receivedMsg) {
        assertTrue(receivedMsg != null);
        final String jsonMsg = (String) receivedMsg.get(0);
        if (receivedMsg.size() <= 2) {
            final List<String> events = new ArrayList<String>();
            events.add("1001,11");
            events.add("1002,12");
            events.add("1003,13");
            events.add("1004,14");
            events.add("1005,15");
            for (int i = 0; i < receivedMsg.size(); i++) {
                assertEquals((String) receivedMsg.get(i), events.get(i));
            }
        } else {
            assertTrue(receivedMsg != null);
        }

    }

    @Test
    public void testJdbcToKafkaSink_Avro() throws Exception {
        prepareTestDataBase(JDBC_INPUT_DATASET);
        configureInputProperties();
        configureKafkaOutputProperties("com.ericsson.component.aia.services.bps.flink.kafka.encoder.FlinkKafkaGenericRecordSerializationSchema",
                "AVRO");
        final ExtendedKafkaOutTopicSubscriber subscriber = new ExtendedKafkaOutTopicSubscriber();
        final Thread subscriberThread = new Thread(subscriber);
        subscriberThread.start();
        final TestJdbcToKafkaExecutor testAppExecutor = new TestJdbcToKafkaExecutor();
        testAppExecutor.start();
        LOGGER.info("**************Sleeping for 10 seconds for starting flink test app**************************");
        testAppExecutor.join(10000);
        LOGGER.info("********Sleeping for 5 seconds**********");
        subscriberThread.join(60000);
        final List<Object> receivedMsg = (List<Object>) subscriber.getMessages();
        validateKafkaAvroOutput(receivedMsg);

    }

    private void validateKafkaAvroOutput(final List<Object> receivedMsg) {
        assertTrue(receivedMsg != null);
    }

    @Test
    public void testJdbcSourceToJdbcSink() throws InterruptedException, IOException, SQLException, ClassNotFoundException {
        prepareTestDataBase(JDBC_INPUT_DATASET);
        final TestJdbcToJdbcExecutor testAppExecutor = new TestJdbcToJdbcExecutor();
        testAppExecutor.start();
        Thread.sleep(10000);
        validateJdbcOutput();
        FileDeleteStrategy.FORCE.deleteQuietly(jdbcTmpDir.toFile());
    }

    private void validateJdbcOutput() throws SQLException {
        final String JDBC_OP = jdbcTmpDir.toAbsolutePath() + SEPARATOR + "h2_JDBC_OP.csv";
        final String query = "call CSVWRITE ('" + JDBC_OP + "', 'SELECT * FROM SALES_OUTPUT', null, null,   '' ) ;";
        File expectedFile = null;
        File actualFile = null;
        Connection connection = null;
        try {
            Class.forName(Driver.class.getCanonicalName());
            connection = DriverManager.getConnection(DB_LOCATION, user, password);
            statement = connection.createStatement();
            statement.executeUpdate(query);
            expectedFile = new File(EXPECTED_CSV_DATA_SET);
            actualFile = new File(JDBC_OP);
            final List<String> expectedLines = FileUtils.readLines(expectedFile);
            final List<String> actualLines = FileUtils.readLines(actualFile);
            Collections.sort(actualLines);
            Collections.sort(expectedLines);
            assertEquals(expectedLines, actualLines);

        } catch (final IOException | SQLException | ClassNotFoundException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        } finally {
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
                System.out.println("Connection closed");
            }
        }
    }

    public class TestKafkaToJdbcExecutor extends Thread {
        @Override
        public void run() {
            LOGGER.info("**************Starting pipeline KAFKA -> Flink -> JDBC **************************");
            configureStepProperties();
            stepProperties.put("schemaRegistry.address", SCHEMA_DIR);
            stepProperties.put("schemaRegistry.cacheMaximumSize", "50");
            stepProperties.put("driver-class", "com.ericsson.component.aia.services.bps.flink.test.app.SampleFlinkStreamingJsonJobRunner");
            final Map<String, Map<String, String>> context = new HashMap<String, Map<String, String>>();
            context.put("attributeIPMap", (Map) kafkaInputProperties);
            context.put("attributeOPMap", (Map) jdbcOutputProperties);
            context.put("attributeStepMap", (Map) stepProperties);
            executePipeLine(context, kafkaTempDir, "KAFKA-To-JDBC");
        }

    }

    public class TestJdbcToJdbcExecutor extends Thread {
        @Override
        public void run() {
            LOGGER.info("**************Starting pipeline JDBC -> Flink -> JDBC **************************");
            configureInputProperties();
            configureOutputProperties();
            jdbcOutputProperties.put("uri", IOURIS.JDBC.getUri() + DB_LOCATION);
            jdbcOutputProperties.put("table.name", OUTPUT_TABLE);
            jdbcOutputProperties.put("output.schema", "com.ericsson.component.aia.services.bps.flink.test.app.TableSchemaPojo");
            configureStepProperties();
            stepProperties.put("driver-class", "com.ericsson.component.aia.services.bps.flink.test.app.SampleFlinkStreamingJsonJobRunner");
            final Map<String, Map<String, String>> context = new HashMap<String, Map<String, String>>();
            context.put("attributeIPMap", (Map) jdbcInputProperties);
            context.put("attributeOPMap", (Map) jdbcOutputProperties);
            context.put("attributeStepMap", (Map) stepProperties);
            executePipeLine(context, jdbcTmpDir, "Jdbc-To-JDBC");
        }

    }

    public class TestJdbcToKafkaExecutor extends Thread {

        @Override
        public void run() {
            LOGGER.info("**************Starting pipeline JDBC -> Flink -> Kafka **************************");
            configureStepProperties();
            stepProperties.put("schemaRegistry.address", SCHEMA_DIR);
            stepProperties.put("schemaRegistry.cacheMaximumSize", "50");
            stepProperties.put("driver-class", "com.ericsson.component.aia.services.bps.flink.test.app.SampleFlinkStreamingJsonJobRunner");
            final Map<String, Map<String, String>> context = new HashMap<String, Map<String, String>>();
            context.put("attributeIPMap", (Map) jdbcInputProperties);
            context.put("attributeOPMap", (Map) kafkaOutputProperties);
            context.put("attributeStepMap", (Map) stepProperties);
            executePipeLine(context, jdbcTmpDir, "Jdbc-To-KAFKA");
        }
    }

}
