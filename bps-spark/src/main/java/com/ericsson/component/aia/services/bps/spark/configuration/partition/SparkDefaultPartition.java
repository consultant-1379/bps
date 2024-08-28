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
package com.ericsson.component.aia.services.bps.spark.configuration.partition;

import static com.ericsson.component.aia.services.bps.core.common.Constants.COMMA;
import static com.ericsson.component.aia.services.bps.core.common.Constants.NUM_PARTITIONS;
import static com.ericsson.component.aia.services.bps.core.common.Constants.PARTITION_COLUMNS;
import static com.ericsson.component.aia.services.bps.core.common.Constants.PARTITION_COLUMNS_DELIMITER;
import static com.ericsson.component.aia.services.bps.core.common.Constants.SAVE_MODE;
import static com.ericsson.component.aia.services.bps.core.common.Constants.TRUE;
import static com.ericsson.component.aia.services.bps.spark.common.AlluxioConfigEnum.SAVE_AS_OBJECT;
import static com.ericsson.component.aia.services.bps.spark.common.Constants.TEXT;

import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.services.bps.core.service.partition.BpsPartition;
import com.ericsson.component.aia.services.bps.spark.utils.SparkUtil;

/**
 * The Class UserDefinedPartition.
 */
@SuppressWarnings("rawtypes")
public class SparkDefaultPartition implements BpsPartition<Dataset> {

    /** Logger. */
    private static final Logger LOG = LoggerFactory.getLogger(SparkDefaultPartition.class);

    /** The partition columns. */
    private String[] partitionColumns;

    /** The properties. */
    private Properties properties;

    /** The data format. */
    private String dataFormat;

    /** SaveMode is used to specify the expected behavior of saving a DataFrame to a data source write mode. */
    private String saveMode;

    /** partition number. */
    private Integer numPartitions;

    /**
     * Initializes UDF partition.
     *
     * @param props
     *            the Properties
     */
    @Override
    public void init(final Properties props) {

        LOG.trace("Entering the init method");
        this.properties = props;
        saveMode = props.getProperty(SAVE_MODE, SaveMode.Append.toString());
        dataFormat = SparkUtil.getFormat(props);
        final String partitionColsDelimiter = props.getProperty(PARTITION_COLUMNS_DELIMITER, COMMA);

        if (StringUtils.isNotBlank(props.getProperty(NUM_PARTITIONS)) && StringUtils.isNumeric(props.getProperty(NUM_PARTITIONS).trim())) {
            numPartitions = Integer.parseInt(props.getProperty(NUM_PARTITIONS).trim());
        }

        if (props.containsKey(PARTITION_COLUMNS) && StringUtils.isNotBlank(props.getProperty(PARTITION_COLUMNS))) {
            partitionColumns = props.getProperty(PARTITION_COLUMNS.trim()).split(partitionColsDelimiter);
        }
        LOG.trace("Exiting the init method");
    }

    /**
     * Writes Dataset to a path based on Partition strategy.
     *
     * @param datasetRecord
     *            the frame
     * @param path
     *            the path
     */
    @SuppressWarnings("unchecked")
    @Override
    public void write(Dataset datasetRecord, final String path) {

        LOG.trace("Entering the write method");
        final Map<String, String> options = SparkUtil.getOptions(properties);

        if (null != numPartitions && numPartitions > 0) {
            datasetRecord = datasetRecord.coalesce(numPartitions);
        }

        final DataFrameWriter dataFrameWriter = datasetRecord.write().format(dataFormat).mode(saveMode).options(options);

        if (TEXT.equalsIgnoreCase(dataFormat)) {
            datasetRecord.toJavaRDD().saveAsTextFile(path);
        } else if (StringUtils.equalsIgnoreCase(properties.getProperty(SAVE_AS_OBJECT.getConfiguration()), TRUE)) {
            datasetRecord.rdd().saveAsObjectFile(path);
        } else if (null != partitionColumns) {
            dataFrameWriter.partitionBy(partitionColumns).save(path);
        } else {
            dataFrameWriter.save(path);
        }
        LOG.trace("Exiting the write method");
    }

    /**
     * Write.
     *
     * @param <T>
     *            the generic type
     * @param rdd
     *            the rdd
     * @param path
     *            the path
     */
    public <T> void write(final JavaRDD<T> rdd, final String path) {
        LOG.trace("Entering the write method");
        rdd.saveAsTextFile(path);
        LOG.trace("Exiting the write method");
    }
}
