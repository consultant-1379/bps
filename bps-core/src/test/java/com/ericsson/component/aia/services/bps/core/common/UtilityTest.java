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

import java.nio.ByteBuffer;

import org.apache.avro.Schema.Type;
import org.junit.Test;

import com.ericsson.component.aia.services.bps.core.common.avro.Utility;

public class UtilityTest {

    @Test
    public void shouldConvertStringToBooelan() {
        final Object trueValue = Utility.getTypeObject(Type.BOOLEAN, "true");
        assertEquals(Boolean.class, trueValue.getClass());
        assertEquals(true, trueValue);

        final Object falseValue = Utility.getTypeObject(Type.BOOLEAN, "false");
        assertEquals(Boolean.class, falseValue.getClass());
        assertEquals(false, falseValue);

        final Object falseValueForNumbericTrue = Utility.getTypeObject(Type.BOOLEAN, "1");
        assertEquals(Boolean.class, falseValueForNumbericTrue.getClass());
        assertEquals(true, falseValueForNumbericTrue);

        final Object falseValueForNumbericFalse = Utility.getTypeObject(Type.BOOLEAN, "0");
        assertEquals(Boolean.class, falseValueForNumbericFalse.getClass());
        assertEquals(false, falseValueForNumbericFalse);

        final Object falseValueForAlphaNumberic = Utility.getTypeObject(Type.BOOLEAN, "1x");
        assertEquals(Boolean.class, falseValueForAlphaNumberic.getClass());
        assertEquals(false, falseValueForAlphaNumberic);

    }

    @Test
    public void shouldConvertStringToByte() {
        final String test = "test";
        final Object trueValue = Utility.getTypeObject(Type.BYTES, test);
        assertEquals(ByteBuffer.wrap(String.valueOf(test).getBytes()), trueValue);

    }

    @Test
    public void shouldConvertStringToNumber() {

        final Object intValue = Utility.getTypeObject(Type.INT, "1");
        assertEquals(Integer.class, intValue.getClass());
        assertEquals(1, intValue);

        final Object expectedNullforInt = Utility.getTypeObject(Type.INT, "1x");
        assertEquals(null, expectedNullforInt);

        final Object longValue = Utility.getTypeObject(Type.LONG, "1");
        assertEquals(Long.class, longValue.getClass());
        assertEquals(1l, longValue);

        final Object expectedNullforLong = Utility.getTypeObject(Type.LONG, "1x");
        assertEquals(null, expectedNullforLong);

        final Object doubleValue = Utility.getTypeObject(Type.DOUBLE, "1");
        assertEquals(Double.class, doubleValue.getClass());
        assertEquals(1D, doubleValue);

        final Object expectedNullforDouble = Utility.getTypeObject(Type.DOUBLE, "1x");
        assertEquals(null, expectedNullforDouble);

        final Object floatValue = Utility.getTypeObject(Type.FLOAT, "1");
        assertEquals(Float.class, floatValue.getClass());
        assertEquals(1F, floatValue);

        final Object expectedNullforFloat = Utility.getTypeObject(Type.FLOAT, "1x");
        assertEquals(null, expectedNullforFloat);

        // NO support for Array it will return as it is.
        final Object arrayValue = Utility.getTypeObject(Type.ARRAY, "1x");
        assertEquals("1x", arrayValue);

        final Object expectedString = Utility.getTypeObject(Type.STRING, '1');
        assertEquals("1", expectedString);

    }

    @Test
    public void shouldConvertIntegerToNumber() {

        final Object intValue = Utility.getTypeObject(Type.INT, 1);
        assertEquals(Integer.class, intValue.getClass());
        assertEquals(1, intValue);

        final Object expectIntValue = Utility.getTypeObject(Type.INT, new Byte("1"));
        assertEquals(Integer.class, expectIntValue.getClass());
        assertEquals(1, expectIntValue);

        final Object longVlaue = Utility.getTypeObject(Type.LONG, 1);
        assertEquals(Long.class, longVlaue.getClass());
        assertEquals(1L, longVlaue);

        final Object expectedLongVlaue = Utility.getTypeObject(Type.LONG, 1L);
        assertEquals(Long.class, expectedLongVlaue.getClass());
        assertEquals(1L, expectedLongVlaue);

        final Object floatValue = Utility.getTypeObject(Type.FLOAT, 1);
        assertEquals(Float.class, floatValue.getClass());
        assertEquals(1.0F, floatValue);

        final Object doubleValue = Utility.getTypeObject(Type.DOUBLE, 1);
        assertEquals(Double.class, doubleValue.getClass());
        assertEquals(1.0D, doubleValue);

        final Object stringValue = Utility.getTypeObject(Type.STRING, 1);
        assertEquals(String.class, stringValue.getClass());
        assertEquals("1", stringValue);

        final Object booleanValue = Utility.getTypeObject(Type.BOOLEAN, 1);
        assertEquals(Boolean.class, booleanValue.getClass());
        assertEquals(true, booleanValue);

        final Object byteValue = Utility.getTypeObject(Type.BYTES, 1);
        assertEquals(ByteBuffer.wrap(String.valueOf("1").getBytes()), byteValue);

        // NO support for Array it will return as it is.
        final Object arrayValue = Utility.getTypeObject(Type.ARRAY, 10000);
        assertEquals(10000, arrayValue);
    }

    @Test
    public void shouldConvertFloatToNumber() {

        final Object intValue = Utility.getTypeObject(Type.INT, 1f);
        assertEquals(Integer.class, intValue.getClass());
        assertEquals(1, intValue);

        final Object longVlaue = Utility.getTypeObject(Type.LONG, 1f);
        assertEquals(Long.class, longVlaue.getClass());
        assertEquals(1L, longVlaue);

        final Object floatValue = Utility.getTypeObject(Type.FLOAT, 1f);
        assertEquals(Float.class, floatValue.getClass());
        assertEquals(1.0F, floatValue);

        final Object expectedFloatValue = Utility.getTypeObject(Type.FLOAT, 1D);
        assertEquals(Float.class, expectedFloatValue.getClass());
        assertEquals(1.0F, expectedFloatValue);

        final Object doubleValue = Utility.getTypeObject(Type.DOUBLE, 1f);
        assertEquals(Double.class, doubleValue.getClass());
        assertEquals(1.0D, doubleValue);

        final Object expectDoubleValue = Utility.getTypeObject(Type.DOUBLE, 1d);
        assertEquals(Double.class, expectDoubleValue.getClass());
        assertEquals(1.0D, expectDoubleValue);

        final Object stringValue = Utility.getTypeObject(Type.STRING, 1f);
        assertEquals(String.class, stringValue.getClass());
        assertEquals("1.0", stringValue);

        final Object booleanValue = Utility.getTypeObject(Type.BOOLEAN, 1f);
        assertEquals(Boolean.class, booleanValue.getClass());
        assertEquals(true, booleanValue);

        final Object byteValue = Utility.getTypeObject(Type.BYTES, 1f);
        assertEquals(ByteBuffer.wrap(String.valueOf(Float.valueOf(1f)).getBytes()), byteValue);

        // NO support for Array it will return as it is.
        final Object arrayValue = Utility.getTypeObject(Type.ARRAY, 10d);
        assertEquals(10d, arrayValue);
    }

    @Test
    public void shouldConvertStringToString() {
        final String test = "test";
        final Object trueValue = Utility.getTypeObject(Type.STRING, test);
        assertEquals(String.class, trueValue.getClass());
        assertEquals(test, trueValue);
    }

    @Test
    public void shouldReturnNullForNull() {
        final Object trueValue = Utility.getTypeObject(Type.STRING, null);
        assertEquals(null, trueValue);
    }
}
