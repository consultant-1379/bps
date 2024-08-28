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
package com.ericsson.component.aia.services.bps.flink.utils;

import static java.sql.Types.BIGINT;
import static java.sql.Types.BOOLEAN;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.FLOAT;
import static java.sql.Types.INTEGER;
import static java.sql.Types.VARCHAR;

import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Ref;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

/**
 * Java to JdbcType Converter class to create table in jdbc database with appropriate data types
 */
@SuppressWarnings("PMD")
public class JavaToJdbcTypeConverter {

    private static final Map<Class<?>, String> TYPES = new HashMap<Class<?>, String>();

    static {
        TYPES.put(String.class, "VARCHAR");
        TYPES.put(double.class, "DOUBLE PRECISION");
        TYPES.put(int.class, "INTEGER");
        TYPES.put(Boolean.class, "BOOLEAN");
        TYPES.put(float.class, "REAL");
        TYPES.put(Date.class, "DATE");
        TYPES.put(Time.class, "TIME");
        TYPES.put(Timestamp.class, "TIMESTAMP");
        TYPES.put(Clob.class, "CLOB");
        TYPES.put(Blob.class, "BLOB");
        TYPES.put(Array.class, "ARRAY");
        TYPES.put(Struct.class, "STRUCT");
        TYPES.put(Ref.class, "REF");
        TYPES.put(URL.class, "DATALINK");
        TYPES.put(long.class, "BIGINT");
        TYPES.put(byte[].class, "VARBINARY");
        TYPES.put(byte.class, "TINYINT");
        TYPES.put(short.class, "SMALLINT");
        TYPES.put(boolean.class, "BIT");
        TYPES.put(BigDecimal.class, "NUMERIC");
    }

    private JavaToJdbcTypeConverter() {
    }

    /**
     * Used to get the correct type for jdbc database
     *
     * @param type
     *            java type coming from schema
     * @return returns appropriate jdbc type to create table in database
     */

    public static String getTypeInfo(final Class<?> type) {
        return TYPES.get(type);
    }

    /**
     * Converts to jdbc types
     *
     * @param name
     *            data type string
     * @return returns jdbc data type
     */

    public static String typeInfo(final String name) {

        switch (name) {
            case "string":
                return "VARCHAR";
            case "int":
            case "INTEGER":
                return "INTEGER";
            case "long":
                return "BIGINT";
            case "double":
                return "DOUBLE PRECISION";
            case "float":
                return "REAL";
            case "Timestamp":
                return "TIMESTAMP";
            case "Date":
                return "DATE";
            case "TIME":
                return "TIME";
            case "bytes":
                return "BYTEA";
            case "byte":
                return "TINYINT";
            default:
                return "VARCHAR";
        }

    }

    /**
     * sql Type conversion
     *
     * @param name
     *            data type name
     * @return returns sql data type
     */
    public static int sqlTypes(final String name) {

        switch (name) {
            case "string":
            case "VARCHAR":
                return VARCHAR;
            case "int":
            case "INTEGER":
                return INTEGER;
            case "long":
            case "BIGINT":
                return BIGINT;
            case "double":
            case "DOUBLE PRECISION":
                return DOUBLE;
            case "BOOLEAN":
            case "BIT":
                return BOOLEAN;
            case "float":
            case "REAL":
                return FLOAT;
            case "Timestamp":
            case "TIMESTAMP":
                return java.sql.Types.TIMESTAMP;
            case "Date":
            case "DATE":
                return java.sql.Types.DATE;
            case "TIME":
                return java.sql.Types.TIME;
            case "bytes":
                return java.sql.Types.BINARY;
            case "byte":
                return java.sql.Types.TINYINT;
            case "VARBINARY":
                return Types.VARBINARY;
            case "TINYINT":
                return Types.TINYINT;
            case "SMALLINT":
                return Types.SMALLINT;
            case "NUMERIC":
                return Types.NUMERIC;
            case "CLOB":
                return Types.CLOB;
            case "BLOB":
                return Types.BLOB;
            case "ARRAY":
                return Types.ARRAY;
            case "STRUCT":
                return Types.STRUCT;
            case "DATALINK":
                return Types.DATALINK;
            case "REF":
                return Types.REF;
            default:
                return VARCHAR;
        }

    }

}