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
package com.ericsson.component.aia.services.bps.spark.common;

/**
 * Various constants used across Bps Spark.
 */
public interface Constants {

    /**
     * The slide window length.
     */
    String SLIDE_WINDOW_LENGTH = "slide.window.length";
    /**
     * The window length.
     */
    String WINDOW_LENGTH = "window.length";

    /** The spark uri. */
    String SPARK_URI = "spark://";

    /** The spark external block store base dir. */
    String SPARK_EXTERNAL_BLOCK_STORE_BASE_DIR = "spark.externalBlockStore.baseDir";

    /** The streaming checkpoint. */
    String STREAMING_CHECKPOINT = "streaming.checkpoint";

    /** The spark external block store url. */
    String SPARK_EXTERNAL_BLOCK_STORE_URL = "spark.externalBlockStore.url";

    /** The spark serializer. */
    String SPARK_SERIALIZER = "spark.serializer";

    /** The spark sql context. */
    String SPARK_SQL_CONTEXT = "sqlContext";

    /** The spark application. */
    boolean SPARK_APPLICATION = true;

    String TEXT = "text";

}
