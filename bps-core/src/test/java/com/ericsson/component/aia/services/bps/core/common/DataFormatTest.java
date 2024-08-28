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
package com.ericsson.component.aia.services.bps.core.common;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * Validate different DataFormat supported by BPS.
 */
public class DataFormatTest {

    @Test
    public void testValidSupportedDataFormat() {
        assertEquals("avro", DataFormat.AVRO.getDataFormat());
        assertEquals("json", DataFormat.JSON.getDataFormat());
        assertEquals("csv", DataFormat.CSV.getDataFormat());
        assertEquals("text", DataFormat.TEXT.getDataFormat());
        assertEquals("SEQUENCEFILE", DataFormat.SEQUENCEFILE.getDataFormat());
        assertEquals("RCFILE", DataFormat.RCFILE.getDataFormat());
        assertEquals("parquet", DataFormat.PARQUET.getDataFormat());
        assertEquals("ORC", DataFormat.ORC.getDataFormat());
        assertEquals("textfile", DataFormat.TEXTFILE.getDataFormat());
        assertEquals("", DataFormat.JDBC.getDataFormat());
    }

}
