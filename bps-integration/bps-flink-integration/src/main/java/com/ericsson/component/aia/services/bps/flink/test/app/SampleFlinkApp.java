/*------------------------------------------------------------------------------
 *******************************************************************************
 * COPYRIGHT Ericsson 2017
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

import com.ericsson.component.aia.common.service.GenericService;
import com.ericsson.component.aia.services.bps.core.service.streams.BpsInputStreams;

/**
 * Interface to execute Kafka or jdbc based on the URI given in context
 *
 */
public interface SampleFlinkApp extends GenericService {

    /**
     * Method to get the particular DataStream of type based on the context name
     *
     * @param bpsInputStreams
     *            input stream from bps
     * @return returns dataStream of the particular type
     */
    DataStream<?> execute(final BpsInputStreams bpsInputStreams);

}
