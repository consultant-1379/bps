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

import static com.ericsson.component.aia.services.bps.core.common.Constants.SEPARATOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;
import org.apache.avro.Schema;
import org.apache.commons.io.FileDeleteStrategy;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.SystemUtils;
import org.apache.curator.test.TestingServer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster;
import org.apache.flink.streaming.connectors.kafka.testutils.ZooKeeperStringSerializer;
import org.h2.Driver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.common.avro.GenericRecordWrapper;
import com.ericsson.component.aia.common.transport.config.KafkaSubscriberConfiguration;
import com.ericsson.component.aia.common.transport.config.PublisherConfiguration;
import com.ericsson.component.aia.common.transport.config.builders.SubscriberConfigurationBuilder;
import com.ericsson.component.aia.common.transport.service.MessageServiceTypes;
import com.ericsson.component.aia.common.transport.service.Publisher;
import com.ericsson.component.aia.common.transport.service.kafka.KafkaFactory;
import com.ericsson.component.aia.common.transport.service.kafka.KafkaSubscriber;
import com.ericsson.component.aia.model.registry.exception.SchemaRetrievalException;
import com.ericsson.component.aia.model.registry.impl.RegisteredSchema;
import com.ericsson.component.aia.model.registry.impl.SchemaRegistryClientFactory;
import com.ericsson.component.aia.services.bps.core.common.uri.IOURIS;
import com.ericsson.component.aia.services.bps.core.exception.BpsRuntimeException;
import com.ericsson.component.aia.services.bps.engine.service.BPSPipeLineExecuter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.producer.KeyedMessage;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import kafka.utils.ZkUtils;

public abstract class AbstractFlinkStreamingJobRunnerTestBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractFlinkStreamingJobRunnerTestBase.class);

    private static final int ZK_TIMEOUT = 30000;

    /**
     * Zookeeper server.
     */
    private static TestingServer zkTestServer;
    /**
     * Kafka server.
     */
    private static KafkaServerStartable kafkaServer;
    /**
     * default kafka port which is randomized using findFreePort.
     */
    public static String kafkaPort;
    /**
     * Default kafka host.
     */
    public static final String LOCAL_HOST = "localhost";

    /**
     * Kafka IN topic for avro formats.
     */
    public static final String KAFKA_AVRO_TOPIC = "radio_session_topic_avro";

    /**
     * Kafka IN topic for JSON formats.
     */
    public static final String KAFKA_JSON_TOPIC = "radio_session_json_topic";

    /**
     * Default kafka OUT topic.
     */
    public static final String KAFKA_OUT_TOPIC = "radio_session_topic_out";

    /**
     * Temporary directory to hold intermediate kafka server and zookeeper data.
     */
    public static Path kafkaTempDir;

    /**
     * default zookeeper port which is randomized using findFreePort.
     */
    public static int zookeeperPort;

    public static final String SCHEMA_DIR = "src/test/resources/avro/";

    private static LocalFlinkMiniCluster cluster;

    protected static Statement statement;

    protected Connection connection;

    public static String DB_LOCATION;

    public static Path jdbcTmpDir;

    private static String driver = Driver.class.getCanonicalName();

    protected static String user = "test";

    protected static String password = "mine";

    protected static String INPUT_TABLE = "SALES";

    protected static String OUTPUT_TABLE = "SALES_OUTPUT";

    private Properties kafkaInputProperties;

    protected enum TestDataFormat {
        AVRO, JSON
    };

    /**
     * Initialize prerequisite associated with test.
     *
     * @throws Exception
     *             if required things unable to setup.
     */
    @SuppressWarnings("PMD.SignatureDeclareThrowsException")
    public static void initKafka() throws Exception {
        zookeeperPort = findFreePort();
        kafkaPort = String.valueOf(findFreePort());
        kafkaTempDir = Files.createTempDirectory("_" + FlinkKafkaStreamingJobRunnerTest.class.getCanonicalName() + System.currentTimeMillis());
        zkTestServer = new TestingServer(zookeeperPort, new File(kafkaTempDir.toFile().getAbsolutePath() + "/tmp_zookeeper"));
        final KafkaConfig config = new KafkaConfig(getBrokerProperties());
        kafkaServer = new KafkaServerStartable(config);
        kafkaServer.startup();
        createTopic(KAFKA_JSON_TOPIC, 1, 1);
        createTopic(KAFKA_AVRO_TOPIC, 1, 1);
        createTopic(KAFKA_OUT_TOPIC, 1, 1);
        startCluster();
    }

    private static Properties getBrokerProperties() {
        final Properties props = new Properties();
        props.put("broker.id", "0");
        props.put("host.name", "localhost");
        props.put("port", kafkaPort);
        props.put("log.dir", kafkaTempDir.toFile().getAbsolutePath() + "/tmp_kafka_dir");
        props.put("zookeeper.connect", zkTestServer.getConnectString());
        props.put("replica.socket.timeout.ms", "1500");
        props.put("num.partitions", "1");
        props.put("offsets.topic.num.partitions", "1");
        return props;
    }

    private static void stopCluster() {
        cluster.stop();
    }

    private static void startCluster() {
        final Configuration configuration = new Configuration();
        configuration.setInteger("taskmanager.rpc.port", findFreePort());
        configuration.setInteger("jobmanager.rpc.port", findFreePort());
        cluster = new LocalFlinkMiniCluster(configuration, false);
        LOGGER.info("********Starting ForkableFlinkMiniCluster**********");
        cluster.start();
    }

    private static Properties getPublisherProperties() {
        final Properties properties = new Properties();
        properties.put("bootstrap.servers", LOCAL_HOST + ":" + kafkaPort);
        properties.put("acks", "1");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return properties;
    }

    protected Properties kafkaInputProperties(final String deSerializationSchemaClass, final String dataFormat, final String topicName) {
        kafkaInputProperties = new Properties();
        kafkaInputProperties.put("uri", "kafka://" + topicName + "?format=" + dataFormat);
        kafkaInputProperties.put("bootstrap.servers", LOCAL_HOST + ":" + kafkaPort);
        kafkaInputProperties.put("zookeeper.connect", LOCAL_HOST + ":" + zookeeperPort);
        kafkaInputProperties.put("version", "10");
        kafkaInputProperties.put("group.id", "radio_session");
        kafkaInputProperties.put("partition.assignment.strategy", "org.apache.kafka.clients.consumer.RangeAssignor");
        if (null != deSerializationSchemaClass) {
            kafkaInputProperties.put("deserialization.schema", deSerializationSchemaClass);
        }
        return kafkaInputProperties;
    }

    private static KeyedMessage<String, GenericRecordWrapper> generateContextRelMessage(final RegisteredSchema registeredSchemaCTX) {
        final GenericRecordWrapper wrapper = getContextRel(registeredSchemaCTX, 0, 30L, 40L, 50L);
        final KeyedMessage<String, GenericRecordWrapper> data = new KeyedMessage<String, GenericRecordWrapper>(KAFKA_AVRO_TOPIC, String.valueOf(50L),
                wrapper);
        return data;
    }

    private static GenericRecordWrapper getContextRel(final RegisteredSchema registeredSchema, final int counts, final Long enbs1apid,
                                                      final Long mmes1ap, final Long globalcid) {
        final Long id = registeredSchema.getSchemaId();
        final Schema schema = registeredSchema.getSchema();
        final ByteBuffer buf = ByteBuffer.allocate(48);
        buf.put("1000".getBytes());
        System.out.println(schema.getName());
        System.out.println(schema.getNamespace());

        final GenericRecordWrapper genericRecord = new GenericRecordWrapper(id, schema);
        genericRecord.put("_NE", "t1");
        genericRecord.put("_TIMESTAMP", 1234567L);
        genericRecord.put("TIMESTAMP_HOUR", 11);
        genericRecord.put("TIMESTAMP_MINUTE", 11);
        genericRecord.put("TIMESTAMP_SECOND", 11);
        genericRecord.put("TIMESTAMP_MILLISEC", 11);
        genericRecord.put("SCANNER_ID", 10l);
        genericRecord.put("RBS_MODULE_ID", 400l);
        genericRecord.put("GLOBAL_CELL_ID", globalcid);
        genericRecord.put("ENBS1APID", enbs1apid);
        genericRecord.put("_ID", 1);
        genericRecord.put("MMES1APID", mmes1ap);
        genericRecord.put("GUMMEI", buf);
        genericRecord.put("RAC_UE_REF", 10l);
        genericRecord.put("TRIGGERING_NODE", 1);
        genericRecord.put("INTERNAL_RELEASE_CAUSE", 345);
        genericRecord.put("S1_RELEASE_CAUSE", 234);
        return genericRecord;
    }

    private static int findFreePort() {
        int port;
        try {
            final ServerSocket socket = new ServerSocket(0);
            socket.setReuseAddress(true);
            port = socket.getLocalPort();
            socket.close();
        } catch (final Exception e) {
            port = -1;
        }
        return port;
    }

    public static void executePipeLine(final Map<String, Map<String, String>> context, final Path target_op, final String scenarioType) {
        
        try{
            LOGGER.info("Creating Flow xml for the test scenario");

            createFlowXml(target_op, context);

            LOGGER.info("Created Flow xml for the test scenario");

            LOGGER.info("Started running pipeline");

            final String flowXML = target_op.toFile().toString() + SEPARATOR + "flow.xml";
      
            BPSPipeLineExecuter.main(new String[] { flowXML });
            
            LOGGER.info("Started running pipeline");
        }
        catch(Exception exp){
            LOGGER.error("Exception occurred while testing Pipe-line application, reason \n", exp);
        }
    }

    private static void createFlowXml(final Path target_op, final Map<String, Map<String, String>> context) {

        try {
            TestUtil.createFolder(target_op);
            TestUtil.createXml("src" + SEPARATOR + "test" + SEPARATOR + "data" + SEPARATOR + "flow.vm", target_op.toFile().toString(), context,
                    "flow.xml");

            LOGGER.info("Initialized Pipe-line successfully, executing pipe-line now!!!");
        } catch (final IOException e) {
            fail(e.getMessage());
        }
    }

    public static void produceMessage(final TestDataFormat dFormat) throws JsonProcessingException, SchemaRetrievalException {
        if (dFormat == TestDataFormat.AVRO) {
            produceAvroRecord();
        } else {
            produceJsonMessage();
        }
    }

    public static void produceAvroRecord() throws SchemaRetrievalException {

        LOGGER.info("**************generating message **************************");
        final Properties schemaRegistryClientProperties = new Properties();
        schemaRegistryClientProperties.put("schemaRegistry.address", SCHEMA_DIR);
        schemaRegistryClientProperties.put("schemaRegistry.cacheMaximumSize", "50");
        final RegisteredSchema registeredSchemaCTX = SchemaRegistryClientFactory.newSchemaRegistryClientInstance(schemaRegistryClientProperties)
                .lookup("celltrace.s.ab11.INTERNAL_PROC_UE_CTXT_RELEASE");
        final KeyedMessage<String, GenericRecordWrapper> data = generateContextRelMessage(registeredSchemaCTX);
        getKafkaPublisher(TestDataFormat.AVRO).sendMessage(KAFKA_AVRO_TOPIC, data.key(), data.message());
        LOGGER.info("**************Sent message*******************");
    }

    public static void produceJsonMessage() throws JsonProcessingException {
        LOGGER.info("**************generating message **************************");
        getKafkaPublisher(TestDataFormat.JSON).sendMessage(KAFKA_JSON_TOPIC, getJSONEvent());
        LOGGER.info("**************Sent message*******************");
    }

    private static Publisher<String, Object> getKafkaPublisher(final TestDataFormat dFormat) {
        final MessageServiceTypes messageServiceTypes = MessageServiceTypes.KAFKA;
        final Properties kafkaProps = getPublisherProperties();
        if (dFormat == TestDataFormat.AVRO) {
            kafkaProps.put("value.serializer", "com.ericsson.component.aia.common.avro.kafka.encoder.KafkaGenericRecordEncoder");
        } else {
            kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        }
        final PublisherConfiguration config = new PublisherConfiguration(kafkaProps, messageServiceTypes);
        final Publisher<String, Object> producer = KafkaFactory.createKafkaPublisher(config);
        return producer;
    }

    private static String getJSONEvent() throws JsonProcessingException {
        final PositionEvent event = new PositionEvent();
        event.setBearing(12.3);
        event.setCarPositionTime(System.currentTimeMillis());
        event.setLatitidue(53.4);
        event.setLongitude(12.4);
        event.setVin("123");
        /*
         * final POJO event = new POJO(); event.setRBS_MODULE_ID(400); event.setSCANNER_ID(10);
         */
        return new ObjectMapper().writeValueAsString(event);
    }

    /**
     * Tear down all services as cleanup.
     *
     * @throws Exception
     *             if unable to tear down successfully.
     */
    public static void tearDownKafka() throws Exception {
        deleteTestTopic(KAFKA_JSON_TOPIC);
        deleteTestTopic(KAFKA_AVRO_TOPIC);
        deleteTestTopic(KAFKA_OUT_TOPIC);
        stopCluster();
        kafkaServer.shutdown();
        kafkaServer.awaitShutdown();
        zkTestServer.stop();
        FileDeleteStrategy.FORCE.deleteQuietly(kafkaTempDir.toFile());
    }

    public static String getFileUri() {
        if (SystemUtils.IS_OS_LINUX || SystemUtils.IS_OS_UNIX) {
            return IOURIS.FILE.getUri();
        } else if (SystemUtils.IS_OS_WINDOWS) {
            return IOURIS.FILE.getUri() + "/";
        } else {
            throw new BpsRuntimeException("OS not supported..Supported OS - LINUX/UNIX/WINDOWS");
        }
    }

    private static ZkUtils getZkUtils() {
        LOGGER.info("In getZKUtils:: zookeeperConnectionString = {}", zkTestServer.getConnectString());
        final ZkClient creator = new ZkClient(zkTestServer.getConnectString(), ZK_TIMEOUT, ZK_TIMEOUT, new ZooKeeperStringSerializer());
        return ZkUtils.apply(creator, false);
    }

    private static void createTopic(final String topic, final int numberOfPartitions, final int replicationFactor) {
        // create topic with one client
        LOGGER.info("Creating topic {}", topic);
        final ZkUtils zkUtils = getZkUtils();
        try {
            AdminUtils.createTopic(zkUtils, topic, numberOfPartitions, replicationFactor, new Properties(), RackAwareMode.Disabled$.MODULE$);
        } finally {
            zkUtils.close();
        }
        LOGGER.info("Topic {} create request is successfully posted", topic);
    }

    private static void deleteTestTopic(final String topic) {
        final ZkUtils zkUtils = getZkUtils();
        try {
            LOGGER.info("Deleting topic {}", topic);
            final ZkClient zk = new ZkClient(zkTestServer.getConnectString(), ZK_TIMEOUT, ZK_TIMEOUT, new ZooKeeperStringSerializer());
            AdminUtils.deleteTopic(zkUtils, topic);
            zk.close();
        } finally {
            zkUtils.close();
        }
    }

    /**
     * @return subscriber configuration.
     */
    private static Properties subscriberConfig() {
        final Properties props = new Properties();
        props.put("bootstrap.servers", LOCAL_HOST + ":" + kafkaPort);
        props.put("group.id", KAFKA_JSON_TOPIC);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("zookeeper.connect", zkTestServer.getConnectString());
        return props;
    }

    public class ExtendedKafkaOutTopicSubscriber implements Runnable {

        List<Object> messages = new LinkedList<>();
        KafkaSubscriber<Object, Object> subscriber;

        /**
         * Default constructor.
         */
        public ExtendedKafkaOutTopicSubscriber() {
            final Properties subscriberConfig = subscriberConfig();
            final KafkaSubscriberConfiguration<Object> conf = SubscriberConfigurationBuilder
                    .<Object, Object> createkafkaConsumerBuilder(subscriberConfig)
                    .addValueDeserializer(subscriberConfig.getProperty("value.deserializer"))
                    .addKeyDeserializer(subscriberConfig.getProperty("key.deserializer")).addTopic(KAFKA_OUT_TOPIC).addProcessors(1).build();
            subscriber = KafkaFactory.createExtendedKafkaSubscriber(conf);
            subscriber.subscribe(Arrays.asList(KAFKA_OUT_TOPIC));
        }

        public List<?> getMessages() {
            return messages;
        }

        @Override
        public void run() {
            LOGGER.info("Extended subscriber Started......");
            try {
                Collection<? extends Object> collectStream = subscriber.collectStream();
                while (collectStream.isEmpty()) {
                    collectStream = subscriber.collectStream();
                }
                messages.addAll(collectStream);
            } finally {
                LOGGER.info("Closing Extended subscriber Thread.");
                subscriber.close();
            }
        }
    }

    /**
     * Validates expected and actual output data.
     *
     * @throws SQLException
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public void validate(final String expectedOutput, final String outputTableName) throws SQLException, ClassNotFoundException, IOException {
        final String query = "call CSVWRITE ( 'JDBC_OP', 'SELECT * FROM " + outputTableName + "', null, null,   '' ) ;";
        validateDBOutput("H2", query, expectedOutput);
    }

    /**
     * Validate Validates expected and actual DB output data.
     *
     * @param dbName
     *            the database name
     * @param query
     *            the query
     * @param expectedFilePath
     *            the expected file path
     * @throws ClassNotFoundException
     * @throws SQLException
     * @throws IOException
     */
    public void validateDBOutput(final String dbName, String query, final String expectedFilePath)
            throws ClassNotFoundException, SQLException, IOException {

        final String JDBC_OP = kafkaTempDir.toAbsolutePath() + SEPARATOR + dbName + "_JDBC_OP.csv";
        query = query.replace("JDBC_OP", JDBC_OP);
        File expectedFile = null;
        File actualFile = null;
        Connection connection = null;
        try {
            Class.forName(driver);
            connection = getDbConnection();
            statement = connection.createStatement();
            statement.executeUpdate(query);
            expectedFile = new File(expectedFilePath);
            actualFile = new File(JDBC_OP);
            final List<String> expectedLines = FileUtils.readLines(expectedFile);
            final List<String> actualLines = FileUtils.readLines(actualFile);
            Collections.sort(actualLines);
            Collections.sort(expectedLines);
            assertEquals(expectedLines.size(), actualLines.size());

        } finally {
            if (connection != null) {
                connection.close();
                System.out.println("Connection closed");
            }
        }
    }

    private static Connection getDbConnection() throws SQLException {
        final String dbUrl = "jdbc:h2:" + kafkaTempDir.toAbsolutePath() + File.separator + "H2_JDBC";
        System.out.println(dbUrl);
        return DriverManager.getConnection(dbUrl, user, password);
    }
}
