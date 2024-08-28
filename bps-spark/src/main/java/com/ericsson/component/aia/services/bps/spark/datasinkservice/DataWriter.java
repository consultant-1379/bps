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
package com.ericsson.component.aia.services.bps.spark.datasinkservice;

import java.io.Serializable;
import java.util.Properties;

/**
 * The abstract <code>DataWriter</code> provides a mechanism Overriding class can choose to output the processed data from BPS step to any external
 * resource such as a message bus, distributed file system, flat file system, databases etc., The Sink Properties are properties that are specified in
 * the flow.xml as part of the <output> tag.
 *
 * @param <T>
 *            the type of the data to be written.
 */
public abstract class DataWriter<T> implements Serializable {

    /**
     * it is required for serialization.
     */
    private static final long serialVersionUID = 3916106963815078101L;
    /**
     * Holds Sink properties.
     */
    private Properties sinkProperties;

    /**
     * The Sink property only available after initialization of writer objects. <b> Do not use {@link DataWriter#getSinkProperties()} in side
     * constructor </b>
     *
     * @return the configuration retrieve from flow.xml for the given sink
     */
    public final Properties getSinkProperties() {
        return sinkProperties;
    }

    /**
     * This method is restricted for overriding to avoid any modification. It is used by BPS framework internally to provide properties to the writer.
     *
     * @param sinkProperties
     *            the sinkProperties to set
     */
    public final void setSinkProperties(final Properties sinkProperties) {
        this.sinkProperties = new Properties();
        this.sinkProperties.putAll(sinkProperties);
    }

    /**
     * Write data to respective sink.
     *
     * @param data
     *            the data to be written
     */
    public abstract void write(final T data);

    /**
     * Clean up.
     */
    public abstract void cleanUp();

}
