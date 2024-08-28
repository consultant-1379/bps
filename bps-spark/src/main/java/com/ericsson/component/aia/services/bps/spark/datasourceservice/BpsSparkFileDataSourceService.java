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
package com.ericsson.component.aia.services.bps.spark.datasourceservice;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;

import com.ericsson.component.aia.services.bps.core.common.uri.IOURIS;
import com.ericsson.component.aia.services.bps.core.datasourceservice.AbstractBpsDataSourceService;
import com.ericsson.component.aia.services.bps.spark.utils.SparkUtil;

/**
 * The <code>BpsSparkFileDataSourceService</code> is responsible for reading data from file system and return respective {@link Dataset } .<br>
 *
 * The <code>BpsSparkFileDataSourceService</code> implements <code>BpsDataSourceService&lt;HiveContext, Dataset&gt;</code> which is specific to
 * HiveContext and Dataset. <br>
 * <br>
 * Example of simple configuration of FileDatasource.
 *
 * <pre>
 *  &lt;input name="file_DataSource_Name"&gt;
 *       &lt;attribute name="URI" value="file:///Absolute_Path" /&gt;    &lt;!--  Representing data source is File type  --&gt;
 *       &lt;attribute name="header" value="true|false" /&gt; &lt;!--  if file type is csv and has first row representing header   --&gt;
 *       &lt;attribute name="inferSchema" value="true|false" /&gt;  &lt;!-- Can infer Schema ?   --&gt;
 *       &lt;attribute name="drop-malformed" value="true" /&gt;  &lt;!--  can drop malformed row ?   --&gt;
 *       &lt;attribute name="dateFormat" value="SimpleDateFormat" /&gt; &lt;!--  how to interpret date   --&gt;
 *       &lt;attribute name="data.format" value="text" /&gt; &lt;!--  date format type   --&gt;
 *       &lt;attribute name="skip-comments" value="true|false" /&gt;  &lt;!--  skip comment part of the file?   --&gt;
 *       &lt;attribute name="quote" value="&amp;quot;" /&gt;
 *       &lt;attribute name="persist" value="false" /&gt;
 *       &lt;!-- If this enabled the data will be materialized , otherwise will dropped after finishing job --&gt;
 *       &lt;attribute name="table-name" value="Table_Name" /&gt; &lt;!--by which provided content can be further utilized in processing phase--&gt;
 * &lt;/input"&gt;
 * </pre>
 *
 */
public class BpsSparkFileDataSourceService extends AbstractBpsDataSourceService<SQLContext, Dataset> {

    /**
     * Gets the service context name as defined in flow xml.
     *
     * @return the service context name
     */
    @Override
    public String getServiceContextName() {
        return IOURIS.FILE.getUri();
    }

    /**
     * The BpsSparkFileDataSourceService#getDataStream(HiveContext, Properties) will return Dataframe based on input configuration.
     */
    @Override
    public Dataset getDataStream() {
        return SparkUtil.getDataFrame(context, properties);
    }
}
