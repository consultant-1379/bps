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
package com.ericsson.component.aia.services.bps.spark.common.avro;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.ericsson.component.aia.services.bps.core.exception.GenericRecordConversionException;

public class GenericRecordConversionExceptionTest {

    @Test
    public void shoudlGenericRecordConversionExceptionPreservedTheExceptionMsg() {
        final GenericRecordConversionException exception = new GenericRecordConversionException("Common-GenericRecordConversionException");
        assertEquals("Common-GenericRecordConversionException", exception.getMessage());
    }

    @Test
    public void shoudlGenericRecordConversionExceptionReturnNullMessageForDefaultException() {
        final GenericRecordConversionException exception = new GenericRecordConversionException();
        assertEquals(null, exception.getMessage());
    }
}