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
package com.ericsson.component.aia.services.bps.core.step;

import java.util.Properties;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.ericsson.component.aia.services.bps.core.service.BpsJobRunner;

/**
 * Created on 10/26/16.
 */
@RunWith(MockitoJUnitRunner.class)
public class BpsDefaultStepTest {

    private BpsDefaultStep bpsDefaultStep = new BpsDefaultStep();

    private Properties properties;

    @Mock
    private BpsJobRunner bpsJobRunner;

    @Before
    public void setUp() {
        properties = new Properties();
        properties.setProperty("master.url", "local[*]");
        properties.setProperty("spark.local.dir", "/tmp/spark.local.dir");
        properties.setProperty(" hive.exec.dynamic.partition.mode", "nonstrict");
        properties.setProperty("spark.serializerl", "org.apache.spark.serializer.KryoSerializer");
        properties.setProperty("spark.externalBlockStore.url", "/tmp/spark.externalBlockStore.url");
        properties.setProperty("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
        properties.setProperty("hive.metastore.warehouse.dir", "/tmp/hive");
        properties.setProperty("spark.externalBlockStore.baseDir", "/tmp/spark.externalBlockStore.baseDir");
        properties.setProperty("hive.exec.scratchdir", "/tmp/hive.exec.scratchdir");
        properties.setProperty("streaming.checkpoint", "/tmp/");
        properties.setProperty("uri", "spark-batch://sales-analysis");

    }

    @Test
    public void testConfigure() {

        final boolean isConfigure = bpsDefaultStep.configure("SPARK_BATCH", properties);
        Assert.assertEquals(isConfigure, true);
    }

    @Test
    public void testExecute() {
        bpsDefaultStep.setStepHandler(bpsJobRunner);
        Mockito.stubVoid(bpsJobRunner).toReturn().on().execute();
        bpsDefaultStep.execute();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIllegalArgumentExceptionOnExecute() {
        bpsDefaultStep.execute();
    }

    @Test
    public void testGetConfiguration() {
        bpsDefaultStep.configure("SPARK_BATCH", properties);
        final Properties props = bpsDefaultStep.getconfiguration();
        Assert.assertEquals(properties, props);
    }

    @Test
    public void testToString() {

        bpsDefaultStep.configure("SPARK_BATCH", properties);
        final String uri = bpsDefaultStep.toString();
        Assert.assertEquals("[StepName=spark-batch:// contextName=SPARK_BATCH]", uri);
    }

    @Test
    public void testGetName() {
        bpsDefaultStep.configure("SPARK_BATCH", properties);
        final String name = bpsDefaultStep.getName();
        Assert.assertEquals("SPARK_BATCH", name);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCleanupWithNullHandler() {
        bpsDefaultStep.cleanUp();
    }

    @Test
    public void testCleanupWithNotNullHandler() {
        bpsDefaultStep.setStepHandler(bpsJobRunner);
        bpsDefaultStep.cleanUp();
    }

}
