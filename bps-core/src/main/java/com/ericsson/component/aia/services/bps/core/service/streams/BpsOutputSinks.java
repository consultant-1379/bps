/**
 *
 * (C) Copyright LM Ericsson System Expertise AT/LMI, 2016
 *
 * The copyright to the computer program(s) herein is the property of Ericsson  System Expertise EEI, Sweden.
 * The program(s) may be used and/or copied only with the written permission from Ericsson System Expertise
 * AT/LMI or in  * accordance with the terms and conditions stipulated in the agreement/contract under which
 * the program(s) have been supplied.
 *
 */
package com.ericsson.component.aia.services.bps.core.service.streams;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.services.bps.core.service.BpsDataSinkService;

/**
 * <code>BpsOutputSinks</code> class holds the collection of {@link BpsDataSinkService} which are configured through flow.xml.
 */
public class BpsOutputSinks implements Serializable {

    /** logger */
    private static final Logger LOG = LoggerFactory.getLogger(BpsOutputSinks.class);

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = -1816113675497026292L;

    /** Collection of StreamWriters for Output stream. */
    private final Map<String, BpsDataSinkService<?, ?>> dataSinks = new HashMap<>();

    /**
     * add method adds a adapter to Bps.
     *
     * @param dataSink
     *            the stream
     */
    public void addBpsDataSinkService(final BpsDataSinkService<?, ?> dataSink) {

        LOG.trace("Adding a StreamWriter {}", dataSink.getDataSinkContextName());

        final String dataSinkContextName = dataSink.getDataSinkContextName().trim().toLowerCase();

        if (dataSinks.containsKey(dataSinkContextName)) {
            throw new IllegalStateException(String.format("Already a data sink %s with this name exists.", dataSinkContextName));
        }

        dataSinks.put(dataSinkContextName, dataSink);
        LOG.trace("Successfully add StreamWriter {}", dataSinkContextName);
    }

    /**
     * Gets the batch streams.
     *
     * @param sinkContextName
     *            the context
     * @return the batch streams
     */
    public BpsDataSinkService<?, ?> getBpsDataSinkService(final String sinkContextName) {
        checkArgument(sinkContextName != null, "SinkContextName cannot be NULL");

        final BpsDataSinkService<?, ?> bpsDataSinkService = dataSinks.get(sinkContextName.trim().toLowerCase());

        if (null != bpsDataSinkService) {
            return bpsDataSinkService;
        }

        throw new IllegalStateException("Could not locate any a Batch streams associated with context specified.");
    }

    /**
     * Write.
     *
     * @param <OUT>
     *            the generic type
     * @param dataStream
     *            the data stream
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public <OUT> void write(final OUT dataStream) {
        LOG.trace("started writing data to output stream");
        for (final BpsDataSinkService dataSink : dataSinks.values()) {
            dataSink.write(dataStream);
        }
        LOG.trace("Successfully written data to output stream");
    }

    /**
     * This method returns all the dataSinkContext names configured.
     *
     * @return the dataSinkContext names configured.
     */
    public Set<String> getDataSinkContextNames() {
        return dataSinks.keySet();
    }

    /**
     * CleanUp method does a cleanup operation for all output streams after writing it to stream.
     */
    public void cleanUp() {
        LOG.trace("cleaning up all output streams");
        for (final BpsDataSinkService<?, ?> dataSink : dataSinks.values()) {
            dataSink.cleanUp();
        }
        LOG.trace("finished cleaning up all output streams");
    }
}
