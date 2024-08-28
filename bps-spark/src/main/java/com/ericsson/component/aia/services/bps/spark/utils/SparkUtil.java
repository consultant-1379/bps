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
package com.ericsson.component.aia.services.bps.spark.utils;

import static com.ericsson.component.aia.services.bps.core.common.Constants.DATA_FORMAT;
import static com.ericsson.component.aia.services.bps.core.common.Constants.FORMAT;
import static com.ericsson.component.aia.services.bps.core.common.Constants.INPUT_TABLE;
import static com.ericsson.component.aia.services.bps.core.common.Constants.PARQUET;
import static com.ericsson.component.aia.services.bps.spark.common.AlluxioConfigEnum.BEAN_CLASS;

import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;

import com.ericsson.component.aia.services.bps.core.common.Constants.BpsLocalAttributesEnum;
import com.ericsson.component.aia.services.bps.core.common.URIDefinition;
import com.ericsson.component.aia.services.bps.core.common.uri.IOURIS;

/**
 * SparkUtil is a utility class for all Spark related operations.
 */
public class SparkUtil {

    private static final String XML_STR = "xml";
    private static final String SPARK_XML = "com.databricks.spark.xml";

    /**
     * Instantiates a new spark util.
     */
    private SparkUtil() {

    }

    /**
     * Creates a dataframe object based on the passed data frame.
     *
     * @param context
     *            the context
     * @param prop
     *            the properties
     * @return the data frame
     */
    public static Dataset getDataFrame(final SQLContext context, final Properties prop) {
        final String tableName = prop.getProperty(INPUT_TABLE);
        final URIDefinition<IOURIS> decode = IOURIS.decode(prop);

        //this creates url path eg: file://xyz apth
        final String path = decode.getProtocol().getUri() + decode.getContext();
        final String format = getFormat(prop, decode);
        final Map<String, String> options = getOptions(prop);
        final Dataset frame = context.read().format(format).options(options).load(path);
        frame.createOrReplaceTempView(tableName);
        return frame;
    }

    /**
     * Creates a dataframe object based on the passed object file.
     *
     * @param <T>
     *            the generic type
     * @param context
     *            the context
     * @param properties
     *            the properties
     * @return the df from object file
     * @throws ClassNotFoundException
     *             the class not found exception
     */
    @SuppressWarnings({ "resource", "unchecked" })
    public static <T> Dataset getDfFromObjectFile(final SQLContext context, final Properties properties) throws ClassNotFoundException {
        final URIDefinition<IOURIS> decode = IOURIS.decode(properties);
        final String path = decode.getProtocol().getUri() + decode.getContext();
        final JavaRDD<T> readRDD = new JavaSparkContext(context.sparkContext()).objectFile(path);
        final Class<T> beanClass = (Class<T>) Class.forName(properties.getProperty(BEAN_CLASS.getConfiguration()));
        return context.createDataFrame(readRDD, beanClass);
    }

    /**
     * Gets the options.
     *
     * @param properties
     *            the properties
     * @return the options
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static Map<String, String> getOptions(final Properties properties) {
        final Map<String, String> options = (Map) properties;
        constructOptions(options);
        return options;
    }

    /**
     * Construct options.
     *
     * @param options
     *            the options
     */
    private static void constructOptions(final Map<String, String> options) {
        for (final BpsLocalAttributesEnum attributesEnum : BpsLocalAttributesEnum.values()) {
            if (options.containsKey(attributesEnum.getAttribute())) {
                options.remove(attributesEnum.getAttribute());
            }
        }
    }

    /**
     * Gets the format based on the passed attribute.
     *
     * First precedence will be given to format present in URI Second precedence will be given to data.format
     *
     * eg: kafka://topic_name?format=json
     *
     * Here URI would be Kafka:// Context would be topic_name Format would be json
     *
     * @param prop
     *            the Properties
     * @param decode
     *            the URIDefinition
     * @return the format
     */
    public static String getFormat(final Properties prop, final URIDefinition<IOURIS> decode) {
        if (prop == null) {
            throw new IllegalArgumentException("Passed Properties file is null");
        }

        String format = prop.getProperty(DATA_FORMAT, PARQUET);
        if (null != decode && !decode.getParams().isEmpty() && decode.getParams().containsKey(FORMAT)
                && StringUtils.isNotEmpty((String) decode.getParams().get(FORMAT))) {
            format = (String) decode.getParams().get(FORMAT);
        }

        if (XML_STR.equalsIgnoreCase(format)) {
            format = SPARK_XML;
        }

        return format;
    }

    /**
     * Gets the format based on the passed attributes.
     *
     * First precedence will be given to format present in URI Second precedence will be given to data.format
     *
     * @param prop
     *            the Properties
     * @return the format
     */
    public static String getFormat(final Properties prop) {
        return getFormat(prop, IOURIS.decode(prop));
    }
}
