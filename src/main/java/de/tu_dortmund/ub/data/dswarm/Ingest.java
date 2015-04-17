/*
The MIT License (MIT)

Copyright (c) 2015, Hans-Georg Becker, http://orcid.org/0000-0003-0432-294X

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
 */

package de.tu_dortmund.ub.data.dswarm;

import java.io.File;
import java.io.StringWriter;
import java.util.Properties;
import java.util.concurrent.Callable;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.entity.mime.content.StringBody;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 * Ingest-Task for Task Processing Unit for d:swarm
 *
 * @author Dipl.-Math. Hans-Georg Becker (M.L.I.S.)
 * @author Jan Polowinski (SLUB Dresden)
 * @version 2015-04-17
 *
 */
public class Ingest implements Callable<String> {

    private Properties config = null;
    private Logger logger = null;

    private String resource;
    private int cnt;

    public Ingest(Properties config, Logger logger, String resource, int cnt) {

        this.config = config;
        this.logger = logger;
        this.resource = resource;
        this.cnt = cnt;
    }

//    @Override
    public String call() {

        // init logger
        PropertyConfigurator.configure(config.getProperty("service.log4j-conf"));

        logger.info("[" + config.getProperty("service.name") + "] " + "Starting 'Ingest (Task)' ...");

        // init IDs of the prototype project
        String dataModelID = config.getProperty("prototype.dataModelID");
//        String projectID = config.getProperty("prototype.projectID");
//        String outputDataModelID = config.getProperty("prototype.outputDataModelID"); // Internal Data Model BiboDocument
//      String updateResourceID = config.getProperty("prototype.resourceID"); // the resource ID to update for each uploaded file
        // use the projects resource as the update-resource for now:
        String updateResourceID = null; try {updateResourceID = getProjectResourceID(dataModelID);} catch (Exception e1) {e1.printStackTrace();}

        // init process values
        String inputResourceID = null;
        String message = null;

        try {
            // build a InputDataModel for the resource
//            String inputResourceJson = uploadFileToDSwarm(resource, "resource for project '" + resource, config.getProperty("project.name") + "' - case " + cnt);
            String inputResourceJson = uploadFileAndUpdateResource(updateResourceID, resource, "resource for project '" + resource, config.getProperty("project.name") + "' - case " + cnt);
            JsonReader jsonReader = Json.createReader(IOUtils.toInputStream(inputResourceJson, "UTF-8"));
            inputResourceID = jsonReader.readObject().getString("uuid");
            logger.info("[" + config.getProperty("service.name") + "] inputResourceID = " + inputResourceID);

            if (inputResourceID != null) {

                if (updateResourceID != null) {

	            	// update the datamodel (will use it's (update) resource)
	            	updateDataModel(dataModelID);
	
	                // we don't need to transform after each ingest of a slice of records,
	            	// so transform and export will be done separately
	            	 logger.info("[" + config.getProperty("service.name") + "] " + "(Note: Only ingest, but no transformation or export done.)");
                }
            }
            
        // no need to clean up resources or datamodels anymore
            
        }
        catch (Exception e) {

            logger.error("[" + config.getProperty("service.name") + "] Processing resource '" + resource + "' failed with a " + e.getClass().getSimpleName());
            e.printStackTrace();
        }

        return message;
    }

    /**
     * update the datamodel with the given ID
     *
     * @param inputDataModelID
     * @return
     * @throws Exception
     */
    private String updateDataModel(String inputDataModelID) throws Exception {

        CloseableHttpClient httpclient = HttpClients.createDefault();
        CloseableHttpResponse httpResponse;

        try {
            // Update the existing input Data Model (we are simply using the example data model here ... TODO !)
            HttpPost httpPost = new HttpPost(config.getProperty("engine.dswarm.api") + "datamodels/" + inputDataModelID + "/data");

            logger.info("[" + config.getProperty("service.name") + "] inputDataModelID : " + inputDataModelID);
            logger.info("[" + config.getProperty("service.name") + "] " + "request : " + httpPost.getRequestLine());

            httpResponse = httpclient.execute(httpPost);

            try {

                int statusCode = httpResponse.getStatusLine().getStatusCode();

                switch (statusCode) {

                    case 200: {

                        logger.info("[" + config.getProperty("service.name") + "] " + statusCode + " : " + httpResponse.getStatusLine().getReasonPhrase());
                       
                        break;
                    }
                    default: {

                        logger.error("[" + config.getProperty("service.name") + "] " + statusCode + " : " + httpResponse.getStatusLine().getReasonPhrase());
                    }
                }
            } finally {
                httpResponse.close();
            }
        } finally {
            httpclient.close();
        }

        return inputDataModelID;
    }

    /**
     * get the resource id of the resource for the data model for the the prototype project
     *
     * @param dataModelID
     * @return resourceID
     * @throws Exception
     */
    private String getProjectResourceID(String dataModelID) throws Exception {

        String resourceID = null;

        CloseableHttpClient httpclient = HttpClients.createDefault();

        try {

            // Hole Mappings aus dem Projekt mit 'projectID'
            HttpGet httpGet = new HttpGet(config.getProperty("engine.dswarm.api") + "datamodels/" + dataModelID);

            CloseableHttpResponse httpResponse = httpclient.execute(httpGet);

            logger.info("[" + config.getProperty("service.name") + "] " + "request : " + httpGet.getRequestLine());

            try {

                int statusCode = httpResponse.getStatusLine().getStatusCode();
                HttpEntity httpEntity = httpResponse.getEntity();

                switch (statusCode) {

                    case 200: {

                        StringWriter writer = new StringWriter();
                        IOUtils.copy(httpEntity.getContent(), writer, "UTF-8");
                        String responseJson = writer.toString();

                        logger.info("[" + config.getProperty("service.name") + "] responseJson : " + responseJson);

                        JsonReader jsonReader = Json.createReader(IOUtils.toInputStream(responseJson, "UTF-8"));
                        JsonObject jsonObject = jsonReader.readObject();
                        JsonArray resources = jsonObject.getJsonObject("configuration").getJsonArray("resources");

                        resourceID = resources.getJsonObject(0).getJsonString("uuid").getString();

                        logger.info("[" + config.getProperty("service.name") + "] resourceID : " + resourceID);

                        break;
                    }
                    default: {

                        logger.error("[" + config.getProperty("service.name") + "] " + statusCode + " : " + httpResponse.getStatusLine().getReasonPhrase());
                    }
                }

                EntityUtils.consume(httpEntity);
            } finally {
                httpResponse.close();
            }

        } finally {
            httpclient.close();
        }

        return resourceID;
    }

    /**
     * upload a file and update an existing resource with it
     *
     * @param resourceUUID
     * @param filename
     * @param name
     * @param description
     * @return responseJson
     * @throws Exception
     */
    private String uploadFileAndUpdateResource(String resourceUUID, String filename, String name, String description) throws Exception {

    	if (null == resourceUUID) throw new Exception("ID of the resource to update was null.");
    	
        String responseJson = null;

        String file = config.getProperty("resource.watchfolder") + File.separatorChar +  filename;

        CloseableHttpClient httpclient = HttpClients.createDefault();

        try {
            HttpPut httpPut = new HttpPut(config.getProperty("engine.dswarm.api") + "resources/" + resourceUUID);

            FileBody fileBody = new FileBody(new File(file));
            StringBody stringBodyForName = new StringBody(name, ContentType.TEXT_PLAIN);
            StringBody stringBodyForDescription = new StringBody(description, ContentType.TEXT_PLAIN);

            HttpEntity reqEntity = MultipartEntityBuilder.create()
                    .addPart("file", fileBody)
                    .addPart("name", stringBodyForName)
                    .addPart("description", stringBodyForDescription)
                    .build();

            httpPut.setEntity(reqEntity);

            logger.info("[" + config.getProperty("service.name") + "] " + "request : " + httpPut.getRequestLine());

            CloseableHttpResponse httpResponse = httpclient.execute(httpPut);

            try {
                int statusCode = httpResponse.getStatusLine().getStatusCode();
                HttpEntity httpEntity = httpResponse.getEntity();

                switch (statusCode) {

                    case 200: {

                        logger.info("[" + config.getProperty("service.name") + "] " + statusCode + " : " + httpResponse.getStatusLine().getReasonPhrase());
                        StringWriter writer = new StringWriter();
                        IOUtils.copy(httpEntity.getContent(), writer, "UTF-8");
                        responseJson = writer.toString();

                        logger.info("[" + config.getProperty("service.name") + "] responseJson : " + responseJson);

                        break;
                    }
                    default: {

                        logger.error("[" + config.getProperty("service.name") + "] " + statusCode + " : " + httpResponse.getStatusLine().getReasonPhrase());
                    }
                }

                EntityUtils.consume(httpEntity);
            } finally {
                httpResponse.close();
            }
        } finally {
            httpclient.close();
        }

        return responseJson;
    }
}
