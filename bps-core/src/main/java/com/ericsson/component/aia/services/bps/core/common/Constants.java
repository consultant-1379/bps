/**
 * (C) Copyright LM Ericsson System Expertise AT/LMI, 2016
 * <p>
 * The copyright to the computer program(s) herein is the property of Ericsson  System Expertise EEI, Sweden.
 * The program(s) may be used and/or copied only with the written permission from Ericsson System Expertise
 * AT/LMI or in  * accordance with the terms and conditions stipulated in the agreement/contract under which
 * the program(s) have been supplied.
 */
package com.ericsson.component.aia.services.bps.core.common;

import java.io.File;

/**
 * Various constants used across Bps libraries.
 */
public interface Constants {

    /** The sql. */
    String SQL = "sql";

    /** The schema. */
    String SCHEMA = "schema";

    /** The table. */
    String TABLE = "table.name";

    /** query for jdbc database. */
    String QUERY = "query";

    /** The input. */
    String INPUT = "input";

    /** The output. */
    String OUTPUT = "output";

    /** The handler. */
    String HANDLER = "handler";

    /** The name. */
    String NAME = "name";

    /** The namespace. */
    String NAMESPACE = "namespace";

    /** The version. */
    String VERSION = "version";

    /** The uri. */
    String URI = "uri";

    /** The master url. */
    String MASTER_URL = "master.url";

    /** The app name. */
    String APP_NAME = "app.name";

    /** The file url. */
    String FILE_URL = "file.path";

    /** The quote. */
    String QUOTE = "quote";

    /** The url sep. */
    String URL_SEP = "//";

    /** The data format. */
    String DATA_FORMAT = "data.format";

    /** The delimiter to use to format data from the source. */
    String TEXT_DELIMITER = "text.delimiter";

    /** The delimiter. */
    String DELIMITER = "delimiter";

    /** The partition columns. */
    String PARTITION_COLUMNS = "partition.columns";

    /** The connection url. */
    String CONNECTION_URL = "connection-url";

    /** The step indicator. */
    String STEP_INDICATOR = ":";

    /** The separator. */
    String SEPARATOR = File.separator;

    /** The ingress. */
    String INGRESS = "ingress";

    /** The egress. */
    String EGRESS = "egress";

    /** The step. */
    String STEP = "step";

    /** The dot. */
    String DOT = ".";

    /** Absolute path to schema file in the file system. */
    String INPUT_SCHEMA_FILE = "input-schema-file";

    /** The uri seperator. */
    String URI_SEPERATOR = "://";

    /** The file uri seperator. */
    String FILE_URI_SEPERATOR = ":///";

    /** The driver class. */
    String DRIVER_CLASS = "driver-class";

    /** The mode. */
    String MODE = "Mode";

    /** The serialization schema. */
    String SERIALIZATION_SCHEMA = "serialization.schema";

    /** The bootstrap servers. */
    String BOOTSTRAP_SERVERS = "bootstrap.servers";

    /** The event type. */
    String EVENT_TYPE = "eventType";

    /** The avro. */
    String AVRO = "avro";

    /** The json. */
    String JSON = "json";

    /** The format. */
    String FORMAT = "format";

    /** The deserialization schema. */
    String DESERIALIZATION_SCHEMA = "deserialization.schema";

    /** The group id. */
    String GROUP_ID = "group.id";

    /** The zookeeper connect. */
    String ZOOKEEPER_CONNECT = "zookeeper.connect";

    /** The schema registry cache maximum size. */
    String SCHEMA_REGISTRY_CACHE_MAXIMUM_SIZE = "schemaRegistry.cacheMaximumSize";

    /** The schema registry address. */
    String SCHEMA_REGISTRY_ADDRESS = "schemaRegistry.address";

    /** The user. */
    String USER = "user";

    /** The password. */
    String PASSWORD = "password";

    /** The driver. */
    String DRIVER = "driver";

    /** The save mode. */
    String SAVE_MODE = "data.save.mode";

    /** The output schema. */
    String OUTPUT_SCHEMA = "output.schema";

    /** The input schema. */
    String INPUT_SCHEMA = "input.schema";

    /** The input table. */
    String INPUT_TABLE = "table-name";

    /** The num partitions. */
    String NUM_PARTITIONS = "numPartitions";

    /** The partition columns delimiter. */
    String PARTITION_COLUMNS_DELIMITER = "partition.columns.delimiter";

    /** The true. */
    String TRUE = "true";

    /** The parquet. */
    String PARQUET = "parquet";

    /** The comma. */
    String COMMA = ",";

    /** The regex. */
    String REGEX = "^([^:].[^:]+?:/{2})(.+\\?*)*(.*)*";
    //prevoius REGEX = "^(.+[^:]+)(:{1}/{2})(.+\\?*)*(.*)*";

    String TIMEOUT = "timeout";

    String URI_FORMAT_STR = "?format=";

    /**
     * The Enum ConfigType.
     */
    enum ConfigType {

        /** The step. */
        STEP("step"),
        /** The input. */
        INPUT("ingress"),
        /** The output. */
        OUTPUT("egress");

        /** The path. */
        private final String path;

        /**
         * Instantiates a new config type.
         *
         * @param path
         *            the path
         */
        ConfigType(final String path) {
            this.path = path;
        }

        /**
         * Gets the path.
         *
         * @return the path
         */
        public String getPath() {
            return path;
        }
    }

    /**
     * BpsLocalAttributesEnum refers all Bps application specific attributes.
     */
    enum BpsLocalAttributesEnum {

        /** The uri. */
        URI(Constants.URI),
        /** The timeout. */
        TIMEOUT(Constants.TIMEOUT),
        /** The driver class. */
        DRIVER_CLASS(Constants.DRIVER_CLASS),
        /** The Schema Registry address. */
        SC_ADDRESS(SCHEMA_REGISTRY_ADDRESS),
        /** The Schema Registry maximum cache size. */
        SC_MAX_CACHE_SIZE(SCHEMA_REGISTRY_CACHE_MAXIMUM_SIZE), NUM_PARTITIONS(Constants.NUM_PARTITIONS),
        /** The partition columns. */
        PARTITION_COLUMNS(Constants.PARTITION_COLUMNS),
        /** The save mode. */
        SAVE_MODE(Constants.SAVE_MODE),
        /** The data format. */
        DATA_FORMAT(Constants.DATA_FORMAT), PARTITION_COLUMNS_DELIMITER(Constants.PARTITION_COLUMNS_DELIMITER);

        /** The attribute. */
        String attribute;

        /**
         * Instantiates a new template attributes enum.
         *
         * @param attribute
         *            the attribute
         */
        BpsLocalAttributesEnum(final String attribute) {
            this.attribute = attribute;
        }

        /**
         * Gets the attribute.
         *
         * @return the attribute
         */
        public String getAttribute() {
            return attribute;
        }
    }
}
