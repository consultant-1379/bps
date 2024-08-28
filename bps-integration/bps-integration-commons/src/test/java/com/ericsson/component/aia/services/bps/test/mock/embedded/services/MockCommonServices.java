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
package com.ericsson.component.aia.services.bps.test.mock.embedded.services;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

import org.apache.commons.io.FileDeleteStrategy;
import org.apache.log4j.Logger;

/**
 * The Class MockCommonServices.
 */
public abstract class MockCommonServices {

    /** The Constant LOGGER. */
    private static final Logger LOGGER = Logger.getLogger(MockCommonServices.class);

    /** The random. */
    protected Random random = new Random();
    /** The tmp dir. */
    protected Path tmpDir;

    /** The counter. */
    private static int counter = 0;

    /** The clean up. */
    protected static boolean cleanUp = true;

    /**
     * Instantiates a new mock common services.
     *
     * @param testScenarioPath
     *            the test scenario path
     */
    public MockCommonServices(final String testScenarioPath) {

        final File fileDir = new File(testScenarioPath);

        if (fileDir.exists()) {
            fileDir.delete();
        }

        fileDir.mkdir();
        tmpDir = fileDir.toPath();
    }

    /**
     * Clean up operation for junit test cases.
     */
    public void cleanUp() {
        if (cleanUp) {
            deleteFolder(tmpDir.toFile());
        }
    }

    /**
     * Delete folder.
     *
     * @param file
     *            the file
     */
    protected void deleteFolder(final File file) {
        try {
            FileDeleteStrategy.FORCE.delete(file);
        } catch (final IOException e) {
            LOGGER.debug("CleanUp, IOException", e);
        }
    }

    /**
     * Gets the current time.
     *
     * @return the current time
     */
    protected String getCurrentTime() {
        ++counter;
        final SimpleDateFormat format = new SimpleDateFormat("dd_M_yyyy_hh_mm_ss");
        return format.format(new Date()) + "_" + counter;
    }
}
