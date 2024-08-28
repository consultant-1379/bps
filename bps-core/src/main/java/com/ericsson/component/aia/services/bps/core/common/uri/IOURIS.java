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
package com.ericsson.component.aia.services.bps.core.common.uri;

import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.ericsson.component.aia.services.bps.core.common.Constants;
import com.ericsson.component.aia.services.bps.core.common.URIDefinition;

/**
 * The <code>IOURIS</code>consists of list of basic Input/Output URI support by BPS. <br>
 * BPS Supports
 * <ul>
 * <li>ActiveMQ</li>
 * <li>Apache Drill</li>
 * <li>File system</li>
 * <li>Apache Kafka</li>
 * <li>Alluxio</li>
 * <li>ZeroMQ</li>
 * <li>HDFS</li>
 * <li>JDBC</li>
 * <li>WEBSOCKET</li>
 * <li>Data Routing</li>
 * </ul>
 */
public enum IOURIS {

    /** The support for Apache ActiveMQ. */
    AMQ("amq://"),
    /** The support for Apache Drill. */
    DRILL("drill://"),
    /** The support for file system. */
    FILE("file://"),
    /** The support for Apache Hive. */
    HIVE("hive://"),
    /** The support for Apache Kafka. */
    KAFKA("kafka://"),
    /** The alluxio. */
    ALLUXIO("alluxio://"),
    /** The zmq. */
    ZMQ("zmq://"),
    /** The hdfs. */
    HDFS("hdfs://"),
    /** The jdbc. */
    JDBC("jdbc://"),
    /** The Spark Machine Learning (ML) */
    ML_SPARK("ml-spark://"),
    /** The websocket. */
    WEBSOCKET("web-socket://"),
    /** The support for Data Routing. */
    DATA_ROUTING("data-routing://");

    /** The uri. */
    private String uri;

    /**
     * Instantiates a new uris.
     *
     * @param uri
     *            Name of the URI.
     */
    IOURIS(final String uri) {
        this.uri = uri;
    }

    /**
     * Gets the uri.
     *
     * @return the String value associated with URI.
     */
    public String getUri() {
        return uri;
    }

    /**
     * Set the URI.
     *
     * @param uri
     *            the new uri
     */
    void setUri(final String uri) {
        this.uri = uri;
    }

    /**
     * This method will return URIS object equivalent to the provided input uri string.
     *
     * @param uri
     *            String value representing URI.
     * @return will return URIS on success else return null.
     */
    public static IOURIS fromString(final String uri) {
        if (uri != null) {
            for (final IOURIS b : IOURIS.values()) {
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
     *            regex to use
     * @return URIDecoder decoded value associated with URI
     */
    public static URIDefinition<IOURIS> decode(final String uri, final String regex) {
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
        return new URIDefinition<IOURIS>(findUri(protocol), content, properties);
    }

    /**
     * This method will try to match the provided uri with supported uris. The uri provided will be split based on ":/" regex. If the first substring
     * of the uri provided matches as a prefix to any of the supported URI, then the corresponding IOURIS will be returned
     *
     * @param uri
     *            The uri provided
     * @return supported URI
     */
    public static IOURIS findUri(final String uri) {
        IOURIS matchingUri = null;
        final String uriName = getUriName(uri);
        for (final IOURIS iouri : IOURIS.values()) {
            if (iouri.getUri().startsWith(uriName)) {
                matchingUri = iouri;
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
     *            of the I/O types
     * @return supported uri name
     */
    public static String getUriName(final String uri) {
        if (uri == null || uri.isEmpty() || !uri.contains(":/")) {
            throw new IllegalArgumentException("Invalid URI");
        }
        final String[] substrings = uri.split(":/", 2);
        return substrings[0].toLowerCase();
    }

    /**
     * This method is the default decoding of URIDefinition. <br>
     *
     * @param properties
     *            Properties
     * @return URIDecoder decoded value associated with URI
     */
    public static URIDefinition<IOURIS> decode(final Properties properties) {
        if (properties == null || !properties.containsKey(Constants.URI)) {
            throw new IllegalArgumentException("URI not found in passed attributes");
        }
        return decode(properties.getProperty(Constants.URI), Constants.REGEX);
    }
}
