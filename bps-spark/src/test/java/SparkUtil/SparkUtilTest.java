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
package SparkUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Properties;

import org.junit.Test;

import com.ericsson.component.aia.services.bps.core.common.Constants;
import com.ericsson.component.aia.services.bps.core.common.uri.IOURIS;
import com.ericsson.component.aia.services.bps.spark.utils.SparkUtil;

public class SparkUtilTest {

    @Test(expected = IllegalArgumentException.class)
    public void testGetFormatForNull() {
        SparkUtil.getFormat(null, null);
        fail("Test case failed");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetFormatForEmpty() {
        SparkUtil.getFormat(new Properties(), IOURIS.decode(new Properties()));
        fail("Test case failed");
    }

    @Test
    public void testGetFormatForHigherPrecedence() {
        final Properties prop = new Properties();
        prop.setProperty(Constants.DATA_FORMAT, Constants.AVRO);
        prop.setProperty(Constants.URI, IOURIS.FILE.getUri() + "testName" + Constants.URI_FORMAT_STR + Constants.JSON);
        final String format = SparkUtil.getFormat(prop, IOURIS.decode(prop));
        assertEquals(Constants.JSON, format);
    }

    @Test
    public void testGetFormatForDataFormatAtribute() {
        final Properties prop = new Properties();
        prop.setProperty(Constants.DATA_FORMAT, Constants.AVRO);
        prop.setProperty(Constants.URI, IOURIS.FILE.getUri() + "testName");
        final String format = SparkUtil.getFormat(prop, IOURIS.decode(prop));
        assertEquals(Constants.AVRO, format);
    }
}
