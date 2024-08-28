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

package com.ericsson.component.aia.services.bps.spark.jobrunner.ml;

import com.ericsson.component.aia.services.bps.core.common.uri.PROCESS_URIS;
import com.ericsson.component.aia.services.bps.spark.jobrunner.BpsSparkStreamingJobRunner;

/**
 * SparkStreamingHandler class is a base class and one of the implementation for Step interface. This handler is used when the user wants to run
 * Streaming application using Spark DStream.
 */

public abstract class SparkMLStreamJobRunner extends BpsSparkStreamingJobRunner {

    private static final long serialVersionUID = 655833713003724650L;

    @Override
    public String getServiceContextName() {
        return PROCESS_URIS.SPARK_ML_STREAMING.getUri();
    }

}
