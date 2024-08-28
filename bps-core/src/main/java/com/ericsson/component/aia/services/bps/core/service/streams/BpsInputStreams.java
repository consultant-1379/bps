/*
 *
 */
package com.ericsson.component.aia.services.bps.core.service.streams;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.services.bps.core.utils.PropertiesReader;

/**
 * The class for holding or referring Bps data source stream reference object for any context.
 */
public class BpsInputStreams implements Serializable {

    /** Logger. */
    private static final Logger LOG = LoggerFactory.getLogger(PropertiesReader.class);

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 1L;

    /** The data points. */
    private final Map<String, BpsBaseStream<?>> dataPoints = new HashMap<>();

    /**
     * Adds a inputstream to dataPoints.
     *
     * @param dataSourceContext
     *            the context
     * @param streamObj
     *            the stream obj
     */
    public void add(String dataSourceContext, final BpsBaseStream<?> streamObj) {

        LOG.trace("Adding a BpsBaseStream " + dataSourceContext);

        dataSourceContext = dataSourceContext.trim().toLowerCase();

        if (dataPoints.containsKey(dataSourceContext)) {
            throw new IllegalStateException(String.format("Already a input stream %s with this name exists.", dataSourceContext));
        }

        dataPoints.put(dataSourceContext, streamObj);
        LOG.trace("Successfully added BpsBaseStream " + dataSourceContext);
    }

    /**
     * Gets the streams.
     *
     * @param <T>
     *            the generic type
     * @param dataSourceContext
     *            the context
     * @return the streams
     */
    @SuppressWarnings("unchecked")
    public <T> BpsStream<T> getStreams(final String dataSourceContext) {
        LOG.trace("Entering the getStreams method ");
        final BpsBaseStream<?> baseStream = dataPoints.get(dataSourceContext.toLowerCase());
        if (null != baseStream) {
            return (BpsStream<T>) baseStream;
        }
        LOG.trace("Returning the getStreams method ");
        throw new IllegalStateException("Could not locate any a streams associated with dataSourceContext specified.");
    }

    /**
     * Gets the stream.
     *
     * @param dataSourceContext
     *            the context
     * @return the properties
     */
    public Properties getProperties(final String dataSourceContext) {
        LOG.trace("Entering the getStreams method ");
        final BpsBaseStream<?> baseSourceService = dataPoints.get(dataSourceContext.toLowerCase());
        if (null != baseSourceService) {
            return baseSourceService.getProperties();
        }
        LOG.trace("Returning the getStreams method ");
        throw new IllegalStateException("Could not locate any a streams associated with dataSourceContext specified.");
    }

    /**
     * This method returns all the dataSourceContext names configured.
     *
     * @return the dataSourceContext names configured.
     */
    public Set<String> getDataSourceContextNames() {
        return dataPoints.keySet();
    }

    /**
     * CleanUp method does a cleanup operation for all output streams after writing it to stream.
     */
    public void cleanUp() {
        LOG.trace("cleaning up all output streams");
        for (final BpsBaseStream<?> dataSourceService : dataPoints.values()) {
            dataSourceService.cleanUp();
        }
        LOG.trace("finished cleaning up all output streams");
    }
}