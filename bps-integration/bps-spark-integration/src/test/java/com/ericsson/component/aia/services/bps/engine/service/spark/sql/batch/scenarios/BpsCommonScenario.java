package com.ericsson.component.aia.services.bps.engine.service.spark.sql.batch.scenarios;

import org.jboss.shrinkwrap.api.importer.ZipImporter;

public interface BpsCommonScenario {

    ZipImporter createJar();
}
