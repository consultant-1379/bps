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
package com.ericsson.component.aia.services.bps.core.common.avro;

import java.nio.ByteBuffer;

import org.apache.avro.Schema.Type;
import org.apache.commons.lang.math.NumberUtils;

/**
 * The <code>Utility</code> class contains common utility methods.
 */
@SuppressWarnings("PMD")
public class Utility {

    private Utility() {

    }

    /**
     * This method use to convert convert and/or get the value as per the type whenever possible. <br>
     *
     * @param type
     *            : expected type
     * @param object
     *            : object that needs to be validated/convert as per the expected type.
     * @return converted or real object as per the type.
     */
    public static Object getTypeObject(final Type type, final Object object) {

        if (object != null) {
            if (object instanceof Number) {
                if (object instanceof Integer || object instanceof Long || object instanceof Byte) {
                    switch (type) {
                        case INT:
                            if (object instanceof Integer) {
                                return object;
                            }
                            return Long.valueOf(object.toString()).intValue();
                        case LONG:
                            if (object instanceof Long) {
                                return object;
                            }
                            return Long.valueOf(object.toString()).longValue();
                        case FLOAT:
                            return Long.valueOf(object.toString()).floatValue();
                        case DOUBLE:
                            return Long.valueOf(object.toString()).doubleValue();
                        case STRING:
                            return String.valueOf(object);
                        case BYTES:
                            return ByteBuffer.wrap(String.valueOf(object).getBytes());
                        case BOOLEAN:
                            // for +ve number return true and for 0 and -ve return false
                            return Long.valueOf(object.toString()).intValue() > 0;
                        default:
                            return object;

                    }

                } else if (object instanceof Double || object instanceof Float) {

                    switch (type) {
                        case INT:
                            return Double.valueOf(object.toString()).intValue();
                        case LONG:
                            return Double.valueOf(object.toString()).longValue();
                        case FLOAT:
                            if (object instanceof Float) {
                                return object;
                            }
                            return Double.valueOf(object.toString()).floatValue();
                        case DOUBLE:
                            if (object instanceof Double) {
                                return object;
                            }
                            return Double.valueOf(object.toString()).doubleValue();
                        case STRING:
                            return String.valueOf(object);
                        case BYTES:
                            return ByteBuffer.wrap(String.valueOf(object).getBytes());
                        case BOOLEAN:
                            // for +ve number return true and for 0 and -ve return false
                            return Double.valueOf(object.toString()).intValue() > 0;
                        default:
                            return object;
                    }
                }
            } else if (object instanceof String || object instanceof Character) {
                final String trim = object.toString().trim();
                switch (type) {
                    case INT:
                        if (NumberUtils.isNumber(trim)) {
                            return Double.valueOf(trim).intValue();
                        }
                        return null;
                    case LONG:
                        if (NumberUtils.isNumber(trim)) {
                            return Double.valueOf(trim).longValue();
                        }
                        return null;
                    case FLOAT:
                        if (NumberUtils.isNumber(trim)) {
                            return Double.valueOf(trim).floatValue();
                        }
                        return null;
                    case DOUBLE:
                        if (NumberUtils.isNumber(trim)) {
                            return Double.valueOf(trim).doubleValue();
                        }
                        return null;
                    case STRING:
                        if (object instanceof String) {
                            return object;
                        }
                        return String.valueOf(object);
                    case BYTES:
                        return ByteBuffer.wrap(String.valueOf(object).getBytes());
                    case BOOLEAN:
                        if (trim.matches("[0-9]*\\.?[0-9]*")) {
                            return Double.valueOf(trim) >= 1 ? true : false;
                        } else if (trim.matches("(?i)(true|false)")) {
                            return Boolean.valueOf(trim);
                        }
                        return false;
                    default:
                        return object;
                }
            }
        }
        return object;

    }
}
