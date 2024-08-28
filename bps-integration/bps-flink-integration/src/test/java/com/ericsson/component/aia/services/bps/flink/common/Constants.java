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
package com.ericsson.component.aia.services.bps.flink.common;

import static com.ericsson.component.aia.services.bps.core.common.Constants.SEPARATOR;

/**
 * Common Constants for the classes
 */
public interface Constants {

    /**
     * The working dir.
     */
    String WORKING_DIR = System.getProperty("user.dir");

    /**
     * The base it folder.
     */
    String BASE_IT_FOLDER = System.getProperty("java.io.tmpdir") + SEPARATOR + "bps_it";

    /**
     * The root base folder.
     */
    String ROOT_BASE_FOLDER = BASE_IT_FOLDER + SEPARATOR + "junit_testing_";

    /**
     * The h2 driver.
     */
    String H2_DRIVER = "org.h2.Driver";

    /**
     * The expected output dir.
     */
    String EXPECTED_OUTPUT_DIR = WORKING_DIR + "/src/test/resources/output/".replace("/", SEPARATOR);

    /**
     * The expected csv data set.
     */
    String EXPECTED_CSV_DATA_SET = EXPECTED_OUTPUT_DIR + "expected_output.csv";

    String EXPECTED_AVRO_DATA_SET = EXPECTED_OUTPUT_DIR + "ExpectedJdbcOutput_AVRO.csv";

    String EXPECTED_JSON_DATA_SET = EXPECTED_OUTPUT_DIR + "ExpectedJdbcOutput_JSON.csv";

    String AVRO_JDBC_TABLE_NAME = "EVENTS_OUTPUT";

    String JSON_JDBC_TABLE_NAME = "POSITION_OUTPUT";

    /** The input data file. */
    String DATA_FILE_NAME = "SalesJan2009";

    /** The input data folder. */
    String INPUT_DATA_FOLDER = WORKING_DIR + "/src/test/resources/data/".replace("/", SEPARATOR);

    String JDBC_INPUT_DATASET = INPUT_DATA_FOLDER + DATA_FILE_NAME + "_JDBC.csv";

    String JDBC_INPUT_EVENT_DATASET = INPUT_DATA_FOLDER + "Events.csv";
}
