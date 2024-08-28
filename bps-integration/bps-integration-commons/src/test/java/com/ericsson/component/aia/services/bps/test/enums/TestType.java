package com.ericsson.component.aia.services.bps.test.enums;

import java.util.EnumSet;

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

import com.ericsson.component.aia.services.bps.core.common.DataFormat;
import com.ericsson.component.aia.services.bps.test.services.TestAlluxioContext;
import com.ericsson.component.aia.services.bps.test.services.TestBaseContext;
import com.ericsson.component.aia.services.bps.test.services.TestFileContext;
import com.ericsson.component.aia.services.bps.test.services.TestHdfsContext;
import com.ericsson.component.aia.services.bps.test.services.TestHiveContext;
import com.ericsson.component.aia.services.bps.test.services.TestJdbcContext;

/**
 * The Enum TestType.
 */
public enum TestType {

    /** The file. */
    FILE(TestFileContext.class, EnumSet.of(DataFormat.CSV, DataFormat.XML, DataFormat.JSON), EnumSet.of(DataFormat.CSV, DataFormat.XML, DataFormat.JSON)),
    /** The hive. */
    HIVE(TestHiveContext.class, EnumSet.of(DataFormat.TEXTFILE), EnumSet.of(DataFormat.TEXTFILE)),
    /** The jdbc. */
    JDBC(TestJdbcContext.class, EnumSet.noneOf(DataFormat.class), EnumSet.noneOf(DataFormat.class)),
    /** The hdfs. */
    HDFS(TestHdfsContext.class, EnumSet.of(DataFormat.CSV,DataFormat.XML, DataFormat.JSON), EnumSet.of(DataFormat.CSV, DataFormat.XML, DataFormat.JSON)),
    /** The alluxio. */
    ALLUXIO(TestAlluxioContext.class, EnumSet.of(DataFormat.CSV,DataFormat.XML, DataFormat.JSON), EnumSet.of(DataFormat.CSV, DataFormat.XML, DataFormat.JSON));

    /** The ref. */
    public Class<? extends TestBaseContext> ref;

    /** The input data formats. */
    private EnumSet<DataFormat> inputDataFormats;

    /** The output data formats. */
    private EnumSet<DataFormat> outputDataFormats;

    /**
     * Instantiates a new test type.
     *
     * @param ref
     *            the ref
     * @param supportedInputFormats
     *            the supported input formats
     * @param supportedOutputFormats
     *            the supported output formats
     */
    TestType(final Class<? extends TestBaseContext> ref, final EnumSet<DataFormat> supportedInputFormats,
             final EnumSet<DataFormat> supportedOutputFormats) {
        this.ref = ref;
        this.inputDataFormats = supportedInputFormats;
        this.outputDataFormats = supportedOutputFormats;
    }

    /**
     * Gets the input data formats.
     *
     * @return the input data formats
     */
    public EnumSet<DataFormat> getInputDataFormats() {
        return inputDataFormats;
    }

    /**
     * Gets the output data formats.
     *
     * @return the output data formats
     */
    public EnumSet<DataFormat> getOutputDataFormats() {
        return outputDataFormats;
    }
}