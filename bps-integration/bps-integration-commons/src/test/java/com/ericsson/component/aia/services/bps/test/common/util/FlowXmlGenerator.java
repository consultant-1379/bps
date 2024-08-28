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
package com.ericsson.component.aia.services.bps.test.common.util;

import static com.ericsson.component.aia.services.bps.core.common.Constants.SEPARATOR;
import static com.ericsson.component.aia.services.bps.test.common.TestConstants.FLOW;
import static com.ericsson.component.aia.services.bps.test.common.TestConstants.FLOW_XML;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.junit.Assert;

/**
 * FlowXmlGenerator class generates flow.xml based on the passed parameters
 */
public class FlowXmlGenerator {

    /** The Constant LOGGER. */
    private static final Logger LOGGER = Logger.getLogger(FlowXmlGenerator.class);

    /**
     * This method creates flow xml based on passed configurations parameters.
     *
     * @param input
     *            the input
     * @param outputFolder
     *            the output folder
     * @param attributeMap
     *            the attribute map
     * @param file
     *            the file
     * @throws Exception
     *             the exception
     */
    public static void createXml(final String input, final String outputFolder, final Map<String, Object> attributeMap, final String file)
            throws Exception {
        writeToFile(input, outputFolder, attributeMap, file);
    }

    /**
     * This method won't create parent folder if it is not exist.
     *
     * @param input
     *            the input
     * @param outputFolder
     *            the output folder
     * @param attributeMap
     *            the attribute map
     * @param file
     *            the file
     * @throws Exception
     *             the exception
     */
    public static void writeToFile(final String input, final String outputFolder, final Map<String, Object> attributeMap, final String file)
            throws Exception {

        new File(outputFolder + SEPARATOR + file).createNewFile();

        final BufferedWriter writer = new BufferedWriter(new FileWriter(outputFolder + SEPARATOR + file));

        initTemplate(writer, input, attributeMap);

        /*
         * flush and cleanup
         */
        writer.flush();
        writer.close();
    }

    public static void initTemplate(final Writer writer, final String input, final Map<String, Object> attributeMap) {
        /* first, get and initialize an engine */
        final VelocityEngine ve = new VelocityEngine();
        final Properties p = new Properties();
        p.setProperty("resource.loader", "class");
        p.setProperty("class.resource.loader.class", "org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader");

        ve.init(p);

        /* next, get the Template */
        final Template template = ve.getTemplate(input);

        /* create a context and add data */
        final VelocityContext context = new VelocityContext();

        // loop a Map
        for (final Map.Entry<String, Object> entry : attributeMap.entrySet()) {
            context.put(entry.getKey(), entry.getValue());
        }

        if (template != null) {
            template.merge(context, writer);
        }
    }

    /**
     * Creates the flow xml based on the scenario.
     *
     * @param target_op
     *            the target op
     * @param context
     *            the context
     */
    public static void createFlowXml(final Path target_op, final Map<String, List<Map<String, String>>> context) {
        try {
            createXml(FLOW_XML, target_op.toFile().toString(), (Map) context, FLOW);
            LOGGER.debug("Create Flow Xml successfully, executing the pipe-line now!!!");
        } catch (final Exception e) {
            LOGGER.info(e);
            Assert.fail(e.getMessage());
        }
    }
}