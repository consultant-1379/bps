package com.ericsson.component.aia.services.bps.test.common;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

/**
 * TestUtil is a utility class for creating flow xmls.
 */
public class TestUtil {

    /** The Constant LOGGER. */
    private static final Logger LOGGER = Logger.getLogger(TestUtil.class);

    /**
     * Creates the folder.
     *
     * @param tmpDir
     *            the tmp dir
     * @return the file
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    public static File createFolderS(final Path tmpDir) throws IOException {
        final File f = new File(tmpDir.toFile().getAbsolutePath());
        f.mkdir();
        return f;
    }

    /**
     * Join files.
     *
     * @param destination
     *            the destination
     * @param sources
     *            the sources
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    public static void joinFiles(final File destination, final File[] sources) throws IOException {
        OutputStream output = null;
        try {
            output = createAppendableStream(destination);
            for (final File source : sources) {
                appendFile(output, source);
            }
        } finally {
            IOUtils.closeQuietly(output);
        }
    }

    /**
     * Creates the appendable stream.
     *
     * @param destination
     *            the destination
     * @return the buffered output stream
     * @throws FileNotFoundException
     *             the file not found exception
     */
    private static BufferedOutputStream createAppendableStream(final File destination) throws FileNotFoundException {
        return new BufferedOutputStream(new FileOutputStream(destination, true));
    }

    /**
     * Append file.
     *
     * @param output
     *            the output
     * @param source
     *            the source
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    private static void appendFile(final OutputStream output, final File source) throws IOException {
        InputStream input = null;
        try {
            input = new BufferedInputStream(new FileInputStream(source));
            IOUtils.copy(input, output);
        } finally {
            IOUtils.closeQuietly(input);
        }
    }

    public static Schema getSchema() throws IOException {
        final String schemaPath = "src/test/resources/avro/celltrace.s.ab11/INTERNAL_PROC_UE_CTXT_RELEASE.avsc";
        return new Schema.Parser().parse(new File(schemaPath));
    }

    public static GenericRecord getGenericRecord() throws IOException {
        final Schema schema = getSchema();
        final GenericRecord genericRecord = new GenericData.Record(schema);
        genericRecord.put("_NE", "testing");
        genericRecord.put("_TIMESTAMP", 123456L);
        genericRecord.put("_ID", 1);
        genericRecord.put("GUMMEI", "bytes".getBytes());
        genericRecord.put("FIELD_1", 12.4f);
        genericRecord.put("FIELD_2", true);
        genericRecord.put("FIELD_3", 212.33d);
        return genericRecord;
    }

    public static List<String> getListOfTestableFields() throws IOException {
        final String[] fieldsStr = new String[] { "_NE", "_TIMESTAMP", "_ID", "GUMMEI", "FIELD_1", "FIELD_2", "FIELD_3" };
        return Arrays.asList(fieldsStr);
    }

    public static Properties getProperties(final String propertiesPath) {

        final Properties prop = new Properties();
        InputStream input = null;

        try {
            input = TestUtil.class.getClassLoader().getResourceAsStream(propertiesPath);
            prop.load(input);
        } catch (final IOException ex) {
            LOGGER.info(ex);
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (final IOException e) {
                    LOGGER.info(e);
                }
            }
        }
        return prop;
    }
}
