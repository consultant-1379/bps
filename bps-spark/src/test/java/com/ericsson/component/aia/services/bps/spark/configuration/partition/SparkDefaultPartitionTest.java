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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.lang.reflect.Field;
import java.util.Properties;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SaveMode;
import org.junit.Test;

import com.ericsson.component.aia.services.bps.core.common.uri.IOURIS;

/**
 * The Class SparkDefaultPartitionTest.
 *
 * Need to redo this part of the test case
 */
public class SparkDefaultPartitionTest {

    private SparkDefaultPartition sdp = new SparkDefaultPartition();

    private static final String OVER_WRITE = SaveMode.Overwrite.toString();
    private static final String FORMAT = "orc";
    private static final String DELIMITER = ";";
    private static final Integer NUM_PARTITIONS = 4;
    private static final String PARTITION_COLUMNS = "test_column";

    // assigning the values
    private Properties initPropertiesFile() {
        final Properties props = new Properties();
        props.setProperty("data.save.mode", OVER_WRITE);
        props.setProperty("data.format", FORMAT);
        props.setProperty("partition.columns.delimiter", DELIMITER);
        props.setProperty("numPartitions", NUM_PARTITIONS.toString());
        props.setProperty("partition.columns", PARTITION_COLUMNS);
        props.setProperty("uri", IOURIS.FILE.getUri() + "test");
        return props;
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testInitMethonForValidScenario() throws IllegalArgumentException, IllegalAccessException, NoSuchFieldException, SecurityException {
        sdp.init(initPropertiesFile());

        final String[] partitionColumns = (String[]) accesssPrivateVariable("partitionColumns").get(sdp);
        final String dataFormat = (String) accesssPrivateVariable("dataFormat").get(sdp);
        final String saveMode = (String) accesssPrivateVariable("saveMode").get(sdp);
        final Integer numPartitions = (Integer) accesssPrivateVariable("numPartitions").get(sdp);

        assertEquals(FORMAT, dataFormat);
        assertEquals(OVER_WRITE, saveMode);
        assertEquals(NUM_PARTITIONS, numPartitions);
        assertEquals(new String[] { PARTITION_COLUMNS }, partitionColumns);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testInitMethonForNullScenario() throws IllegalArgumentException, IllegalAccessException, NoSuchFieldException, SecurityException {
        final Properties props = new Properties();
        props.setProperty("uri", IOURIS.FILE.getUri() + "test");
        sdp.init(props);

        final String[] partitionColumns = (String[]) accesssPrivateVariable("partitionColumns").get(sdp);
        final String dataFormat = (String) accesssPrivateVariable("dataFormat").get(sdp);
        final String saveMode = (String) accesssPrivateVariable("saveMode").get(sdp);
        final Integer numPartitions = (Integer) accesssPrivateVariable("numPartitions").get(sdp);

        assertEquals("parquet", dataFormat);
        assertEquals(SaveMode.Append.toString(), saveMode);
        assertEquals(null, numPartitions);
        assertEquals(null, partitionColumns);
    }

    @Test
    public void writeShouldCallSaveAsTextFile() {
        final JavaRDD<?> record = mock(JavaRDD.class);
        final String path = "path";
        sdp.write(record, path);
        verify(record, times(1)).saveAsTextFile(path);
    }

    private Field accesssPrivateVariable(final String variable) throws NoSuchFieldException, SecurityException {
        final Field privateVariable = SparkDefaultPartition.class.getDeclaredField(variable);
        privateVariable.setAccessible(true);
        return privateVariable;
    }
}
