/**
 * (C) Copyright LM Ericsson System Expertise AT/LMI, 2016
 * <p>
 * The copyright to the computer program(s) herein is the property of Ericsson  System Expertise EEI, Sweden.
 * The program(s) may be used and/or copied only with the written permission from Ericsson System Expertise
 * AT/LMI or in  * accordance with the terms and conditions stipulated in the agreement/contract under which
 * the program(s) have been supplied.
 */
package com.ericsson.component.aia.services.bps.test.cluster.util;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;

/**
 * Makes a rest GET request to the spark master, fetches the submitted job status and returns the following driver state. <br>
 * ERROR - If there jar or file transferred on HDFS does not exists</br>
 * <br>
 * RUNNING - If the Submitted job still performing the operations </br>
 * <br>
 * FAILED - If there is any exception thrown during performing operations. </br>
 * <br>
 * FINISHED - If Submitted job executed successfully. </br>
 *
 *
 */
public class SubmitJobStatus {

    private static final Logger LOGGER = Logger.getLogger(SubmitJobStatus.class);

    public static String getJobDriverState(final String urlPath) throws Exception {
        String driverState = "";
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        final Callable<String> callable = new HttpUrlRequestGetClientTask(urlPath);
        final Future<String> future = executor.submit(callable);
        try {
            driverState = future.get();
            System.out.println(future.get(300, TimeUnit.SECONDS));
            LOGGER.info("Driver State is :" + driverState);
        } catch (TimeoutException e) {
            future.cancel(true);
            e.printStackTrace();
        }
        executor.shutdownNow();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        return driverState;
    }
}

class HttpUrlRequestGetClientTask implements Callable<String> {

    private static final Logger LOGGER = Logger.getLogger(HttpUrlRequestGetClientTask.class);

    private String urlPath;

    public HttpUrlRequestGetClientTask(final String urlPath) {
        this.urlPath = urlPath;
    }

    @Override
    public String call() throws Exception {

        String driverState = "";
        while (!Thread.interrupted()) {
            driverState = HttpUrlRequestPostClient.getJobStatus(urlPath);
            Thread.sleep(5000);
            if (driverState.equalsIgnoreCase("RUNNING")) {
                LOGGER.info("Spark, running Job status :: " + driverState + "...");
            } else {
                LOGGER.info("Spark, running Job Completed with Status :" + driverState);
                return driverState;
            }
        }
        return driverState;
    }
}