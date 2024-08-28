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

import org.apache.flink.streaming.api.datastream.DataStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.common.service.loader.GenericServiceLoader;
import com.ericsson.component.aia.services.bps.core.common.Constants;
import com.ericsson.component.aia.services.bps.core.common.uri.IOURIS;
import com.ericsson.component.aia.services.bps.flink.jobrunner.BpsFlinkStreamingJobRunner;

/**
 * This class extends {@link BpsFlinkStreamingJobRunner} and provides logic for reading {@link DataStream} from data sources and applying
 * transformation and writing the result to configured data sinks
 */
public class SampleFlinkStreamingJobRunner extends BpsFlinkStreamingJobRunner {

    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = LoggerFactory.getLogger(SampleFlinkStreamingJobRunner.class);

    @Override
    public void executeJob() {

        final String uri = getBpsInputStreams().getProperties("input-stream").getProperty(Constants.URI);
        final String ioUri = IOURIS.findUri(uri).getUri();
        final SampleFlinkApp dataStreamSource = (SampleFlinkApp) GenericServiceLoader.getService(SampleFlinkApp.class, ioUri);
        final DataStream<?> dataStream = dataStreamSource.execute(getBpsInputStreams());
        persistDataStream(dataStream);
        LOGGER.info("EpsFlinkStreamingHandler executeJob successfully completed");

    }

    @Override
    public void cleanUp() {

    }
}