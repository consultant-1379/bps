package com.ericsson.component.aia.services.bps.flink.utils;

/**
 * SaveMode is used to specify the expected behavior of saving a DataFrame to a data source.
 *
 * @since 1.1.4
 */
public enum SaveMode {

    /**
     * Append mode means that when saving a DataFrame to a data source, if data/table already exists, contents of the DataStream are expected to be
     * appended to existing data.
     *
     * @since 1.1.4
     */
    Append,
    /**
     * Overwrite mode means that when saving a DataFrame to a data source, if data/table already exists, existing data is expected to be overwritten
     * by the contents of the DataStream.
     *
     * @since 1.1.4
     */
    Overwrite,

}
