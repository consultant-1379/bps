/**
 * (C) Copyright LM Ericsson System Expertise AT/LMI, 2016
 * <p>
 * The copyright to the computer program(s) herein is the property of Ericsson  System Expertise EEI, Sweden.
 * The program(s) may be used and/or copied only with the written permission from Ericsson System Expertise
 * AT/LMI or in  * accordance with the terms and conditions stipulated in the agreement/contract under which
 * the program(s) have been supplied.
 */
package com.ericsson.component.aia.services.bps.engine.service.spark.sql.batch.jar.util;

import static org.junit.Assert.fail;

import java.io.File;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.importer.ZipImporter;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.jboss.shrinkwrap.resolver.api.maven.Maven;

import com.ericsson.component.aia.services.bps.engine.service.spark.sql.batch.scenarios.BpsCommonScenario;

/**
 * Creates Bps spark dependencies in order to execute the job on spark master.
 */

public class SparkBpsJarAssembler implements BpsCommonScenario {
    private static final Logger LOGGER = Logger.getLogger(SparkBpsJarAssembler.class);
    private final static String BPS_TEST_PACKAGE = "com.ericsson.component.aia.services.bps.test";

    public ZipImporter createJar() {
        String executionPath = System.getProperty("user.dir");
        int separatorIndex = StringUtils.lastOrdinalIndexOf(System.getProperty("user.dir"), "/", 2);
        executionPath = executionPath.substring(0, separatorIndex);
        String[] bpsPOMXmls = { executionPath + "/bps-core/" + "pom.xml", executionPath + "/bps-spark/" + "pom.xml",
                executionPath + "/bps-engine/" + "pom.xml", };
        // Create and add classes to the JAR

        ZipImporter bpsDependenciesJar = null;
        try {
            bpsDependenciesJar = constructTestJar(executionPath, bpsPOMXmls);

        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
        return bpsDependenciesJar;
    }

    private static ZipImporter constructTestJar(final String executionPath, final String[] bpsPOMXmls) {
        final ZipImporter bpsDependenciesJar;
        final JavaArchive lib = ShrinkWrap.create(JavaArchive.class, "test.jar").addPackages(true, BPS_TEST_PACKAGE);
        for (int i = 0; i < bpsPOMXmls.length; i++) {
            LOGGER.info("==============bps pom.xml==================" + bpsPOMXmls[i]);
            JavaArchive[] jar = doImport(bpsPOMXmls[i]);
            for (JavaArchive tmp : jar) {
                lib.merge(tmp);
            }
        }
        bpsDependenciesJar = lib.as(ZipImporter.class);
        bpsDependenciesJar.importFrom(new File(executionPath + "/bps-core" + "/target/" + "bps-core.jar"))
                .importFrom(new File(executionPath + "/bps-spark" + "/target/" + "bps-spark.jar"))
                .importFrom(new File(executionPath + "/bps-engine" + "/target/" + "bps-engine.jar")).as(JavaArchive.class);

        return bpsDependenciesJar;
    }

    private static JavaArchive[] doImport(String pomFile) {
        final JavaArchive[] dependencies = Maven.configureResolver().loadPomFromFile(pomFile).importRuntimeDependencies().resolve().withTransitivity()
                .as(JavaArchive.class);
        return dependencies;

    }
}
