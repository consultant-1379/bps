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
package com.ericsson.component.aia.services.bps.engine.parser;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Serializable;
import java.io.StringReader;
import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.services.bps.core.common.uri.IOURIS;
import com.ericsson.component.aia.services.bps.core.pipe.BpsPipe;

/**
 * This class will read flow xml and configures the pipe line to be executed.
 */
public class ExecuterHelper implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(ExecuterHelper.class);

    private static final String HDFS_NAMENODE_URI_REGEX = "^(hdfs:/{2}.+?:{1}[0-9]*+)";

    private static final Pattern HDFS_NAMENODE_URI_PATTERN = Pattern.compile(HDFS_NAMENODE_URI_REGEX);

    /**
     * This method will parse the flowxml and build an pipe line for execution
     *
     * @param flowXML
     *            to read i/o and step configuration
     * @return an pipe line for execution
     *
     * @throws FileNotFoundException
     *             if flow xml is not found
     */
    public BpsPipe init(final String flowXML) throws FileNotFoundException {

        Reader reader = null;
        if (StringUtils.startsWithIgnoreCase(flowXML, IOURIS.HDFS.getUri())) {

            reader = new StringReader(readXmlFromHdfs(flowXML));
        } else {
            final File file = new File(flowXML);
            checkArgument(file.exists() && file.isFile(), "Invalid flow xml file path, please provide valid path");
            reader = new FileReader(file);
        }

        return BpsModelParser.parseFlow(reader);
    }

    private String readXmlFromHdfs(final String flowXML) {
        final StringBuilder xmlStr = new StringBuilder();
        final FileSystem filesys;
        try {
            final Matcher matcher = HDFS_NAMENODE_URI_PATTERN.matcher(flowXML);
            if (matcher.find()) {
                final String uri = matcher.group();
                LOGGER.trace("HDFS NAME NODE URI::{}", uri);
                filesys = FileSystem.get(new URI(uri), new Configuration());
            } else {
                filesys = FileSystem.get(new Configuration());
            }
            final Path path = new Path(flowXML);
            final BufferedReader bufferReader = new BufferedReader(new InputStreamReader(filesys.open(path)));
            String currentLine;
            while ((currentLine = bufferReader.readLine()) != null) {
                xmlStr.append(currentLine).append("\n");
            }
        } catch (final Exception e) {
            LOGGER.error("Got exception while reading Xml from Hdfs ", e);
            throw new IllegalStateException("Unable to parse provided flow xml.", e);
        }

        return xmlStr.toString();
    }
}
