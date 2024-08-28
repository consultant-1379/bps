/**
 *
 * (C) Copyright LM Ericsson System Expertise AT/LMI, 2016
 *
 * The copyright to the computer program(s) herein is the property of Ericsson  System Expertise EEI, Sweden.
 * The program(s) may be used and/or copied only with the written permission from Ericsson System Expertise
 * AT/LMI or in  * accordance with the terms and conditions stipulated in the agreement/contract under which
 * the program(s) have been supplied.
 *
 */
package com.ericsson.component.aia.services.bps.core.common.uri;

import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.ericsson.component.aia.services.bps.core.common.URIDefinition;

/**
 * The <code>ProcessURIS</code>consists of list of basic Step's URI support by BPS. <br>
 * BPS Supports
 * <ul>
 * <li>SPARK SQL</li>
 * <li>SPARK BATCH</li>
 * <li>SPARK BATCH SQL</li>
 * <li>SPARK STREAMING</li>
 * </ul>
 *
 */
public enum PROCESS_URIS {

    SPARK_SQL("spark-sql://"),
    /** The spark batch SQL. */
    SPARK_BATCH_SQL("spark-batch-sql://"),
    /** The spark batch. */
    SPARK_BATCH("spark-batch://"),
    /** The spark streaming. */
    SPARK_STREAMING("spark-streaming://"),
    /** The ML streaming. */
    SPARK_ML_STREAMING("spark-ml-streaming://"),
    /** The ML Batch. */
    SPARK_ML_BATCH("spark-ml-batch://"),
    /** The flink streaming. */
    FLINK_STREAMING("flink-streaming://");

    /** The uri. */
    private String processUri;

    /**
     * Instantiates a new uris.
     *
     * @param uri
     *            Name of the URI.
     */
    PROCESS_URIS(final String uri) {
        this.processUri = uri;
    }

    /**
     * Gets the uri.
     *
     * @return the String value associated with URI.
     */
    public String getUri() {
        return processUri;
    }

    /**
     * Set the URI.
     *
     * @param uri
     *            the new uri
     */
    void setUri(final String uri) {
        this.processUri = uri;
    }

    /**
     *
     * This method will return URIS object equivalent to the provided input uri string.
     *
     * @param uri
     *            String value representing URI.
     * @return will return URIS on success else return null.
     */
    public static PROCESS_URIS fromString(final String uri) {
        if (uri != null) {
            for (final PROCESS_URIS b : PROCESS_URIS.values()) {
                if (uri.equalsIgnoreCase(b.getUri())) {
                    return b;
                }
            }
        }
        throw new IllegalArgumentException("Unknow URI type requested" + uri);
    }

    /**
     * Decode the string representing URI value. <br>
     *
     * @param uri
     *            Valid URI
     * @param regex
     *            regex used to decode uri
     * @return URIDecoder decoded value associated with URI
     */
    @SuppressWarnings("CPD-START")
    public static URIDefinition<PROCESS_URIS> decode(final String uri, final String regex) {
        final Pattern pattern = Pattern.compile(regex);

        final Matcher matcher = pattern.matcher(uri);
        matcher.find();
        final String protocol = matcher.group(1);
        String content = matcher.group(2);
        final String[] split = content.split("\\?");
        final Properties properties = new Properties();
        if (split.length == 2) {
            content = split[0].trim();
            if (content.isEmpty()) {
                throw new IllegalArgumentException("Context Name can not be null");
            }

            final String[] parms = split[1].split("\\&");
            for (final String parm : parms) {
                final String[] keyValuePair = parm.split("\\=");
                if (keyValuePair.length == 2) {
                    properties.put(keyValuePair[0], keyValuePair[1]);
                }
            }
        }

        return new URIDefinition<PROCESS_URIS>(fromString(protocol), content, properties);
    }

    /**
     * This method will try to match the provided uri with supported uris. The uri provided will be split based on ":/" regex. If the first substring
     * of the uri provided matches as a prefix to any of the supported URI, then the corresponding PROCESS_URIS will be returned
     *
     * @param uri
     *            The uri provided
     * @return supported URI
     */
    public static PROCESS_URIS findUri(final String uri) {
        PROCESS_URIS matchingUri = null;
        final String uriName = getUriName(uri);
        for (final PROCESS_URIS processUris : PROCESS_URIS.values()) {
            if (processUris.getUri().startsWith(uriName)) {
                matchingUri = processUris;
            }
        }
        if (matchingUri == null) {
            throw new IllegalArgumentException("URI not supported");
        }
        return matchingUri;
    }

    /**
     * This method will extract the uriName from uri
     *
     * @param uri
     *            The uri provided
     * @return supported uri name
     */
    public static String getUriName(final String uri) {
        if (uri == null || uri.isEmpty() || !uri.contains(":/")) {
            throw new IllegalArgumentException("Invalid URI");
        }
        final String[] substrings = uri.split(":/", 2);
        return substrings[0].toLowerCase();
    }
}
