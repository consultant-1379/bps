/**
 * (C) Copyright LM Ericsson System Expertise AT/LMI, 2017
 * <p>
 * The copyright to the computer program(s) herein is the property of Ericsson  System Expertise EEI, Sweden.
 * The program(s) may be used and/or copied only with the written permission from Ericsson System Expertise
 * AT/LMI or in  * accordance with the terms and conditions stipulated in the agreement/contract under which
 * the program(s) have been supplied.
 */
package com.ericsson.component.aia.services.bps.test.cluster.util;

import static com.ericsson.component.aia.services.bps.test.common.TestConstants.FLOW_XML;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;

import org.json.JSONObject;
import org.junit.Assert;
import org.skyscreamer.jsonassert.JSONParser;

import com.ericsson.component.aia.services.bps.test.common.util.FlowXmlGenerator;

/**
 * Helps to create submission request to spark master and get response of the running job.
 */
public class HttpUrlRequestPostClient {

    public static String CreateSubmissionResponse(final String urlPath, final Map<String, String> context, String requestFor) {

        String submissionId = null;

        try {
            URL url = new URL(urlPath);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setDoOutput(true);
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            StringWriter writer = new StringWriter();

            FlowXmlGenerator.initTemplate(writer, FLOW_XML.replace("flow", requestFor), (Map) context);
            System.out.println("===================writer.toString()==========\n" + writer.toString());
            OutputStream os = conn.getOutputStream();
            os.write(writer.toString().getBytes());
            os.flush();
            if (conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
                throw new RuntimeException("Failed : HTTP error code : " + conn.getResponseCode());
            }
            BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
            StringBuilder jsonReponse = new StringBuilder();
            String output;
            System.out.println("Output from Server .... \n");
            while ((output = br.readLine()) != null) {
                jsonReponse.append(output);
            }
            JSONObject jsonObj = (JSONObject) JSONParser.parseJSON(jsonReponse.toString());
            submissionId = jsonObj.getString("submissionId");
            conn.disconnect();

        } catch (MalformedURLException e) {
            e.printStackTrace();
            Assert.fail("Submitted Job Failed to execute:" + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Submitted Job Failed to execute:" + e.getMessage());
        }

        return submissionId;

    }

    public static String getJobStatus(final String urlPath) {
        String driverState = null;

        try {

            URL url = new URL(urlPath);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("Accept", "application/json");

            if (conn.getResponseCode() != 200) {
                throw new RuntimeException("Failed : HTTP error code : " + conn.getResponseCode());
            }

            BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
            StringBuilder jsonReponse = new StringBuilder();
            String output;
            while ((output = br.readLine()) != null) {
                jsonReponse.append(output);
            }
            JSONObject jsonObj = (JSONObject) JSONParser.parseJSON(jsonReponse.toString());
            driverState = jsonObj.getString("driverState");
            conn.disconnect();
        } catch (MalformedURLException e) {
            e.printStackTrace();
            Assert.fail("Submitted Job Failed to execute:" + e.getMessage());

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Submitted Job Failed to execute:" + e.getMessage());
        }
        return driverState;

    }

}