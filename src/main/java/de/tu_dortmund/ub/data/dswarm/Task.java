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

import de.tu_dortmund.ub.data.util.XmlTransformer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.http.Consts;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.entity.mime.content.StringBody;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.jdom2.Document;
import org.jdom2.input.SAXBuilder;
import org.jdom2.output.Format;
import org.jdom2.output.XMLOutputter;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;

import org.apache.http.client.methods.HttpPut;

import java.io.*;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;

/**
 * Task for Task Processing Unit for d:swarm
 *
 * @author Dipl.-Math. Hans-Georg Becker (M.L.I.S.)
 * @version 2015-03-19
 *
 */
public class Task implements Callable<String> {

    private Properties config = null;
    private Logger logger = null;

    private String resource;
    private int cnt;

    public Task(Properties config, Logger logger, String resource, int cnt) {

        this.config = config;
        this.logger = logger;
        this.resource = resource;
        this.cnt = cnt;
    }

//    @Override
    public String call() {

        // init logger
        PropertyConfigurator.configure(config.getProperty("service.log4j-conf"));

        logger.info("[" + config.getProperty("service.name") + "] " + "Starting 'Task' ...");

        // init IDs of the prototype project
        String dataModelID = config.getProperty("prototype.dataModelID");
        String projectID = config.getProperty("prototype.projectID");
        String outputDataModelID = config.getProperty("prototype.outputDataModelID"); // Internal Data Model BiboDocument
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

                    // don't transform after each ingest
                    
//                    if (inputDataModelID != null) {
//                        // configuration and processing of the task
//                        String jsonResponse = executeTask(inputDataModelID, projectID, resourceID, outputDataModelID);
//
//                        if (jsonResponse != null) {
//
//                            if (Boolean.parseBoolean(config.getProperty("results.persistInFolder"))) {
//
//                                // save results in files
//                                FileUtils.writeStringToFile(new File(config.getProperty("results.folder") + File.separatorChar + inputDataModelID + ".json"), jsonResponse);
//
//                                message = "'" + resource + "' transformed. results in '" + config.getProperty("results.folder") + File.separatorChar + inputDataModelID + ".json" + "'";
//                            }
//                        } else {
//
//                            message = "'" + resource + "' not transformed: error in task execution.";
//                        }
//                    }
 
                }
            }

            // no need to clean up resources or datamodels anymore, but: TODO: clean the configurations which seem to be created for each update
        }
        catch (Exception e) {

            logger.error("[" + config.getProperty("service.name") + "] Processing resource '" + resource + "' failed with a " + e.getClass().getSimpleName());
            e.printStackTrace();
        }

        return message;
    }

    /**
     * cleanup data model and resource in d:swarm
     *
     * @param inputResourceID
     * @param inputDataModelID
     */
    private void cleanup(String inputResourceID, String inputDataModelID) throws Exception {

        CloseableHttpClient httpclient = HttpClients.createDefault();

        try {

            if (inputDataModelID != null) {

                HttpDelete httpDelete = new HttpDelete(config.getProperty("engine.dswarm.api") + "datamodels/" + inputDataModelID);

                CloseableHttpResponse httpResponse = httpclient.execute(httpDelete);

                logger.info("[" + config.getProperty("service.name") + "] " + "request : " + httpDelete.getRequestLine());

                try {

                    int statusCode = httpResponse.getStatusLine().getStatusCode();
                    HttpEntity httpEntity = httpResponse.getEntity();

                    switch (statusCode) {

                        case 204: {

                            logger.info("[" + config.getProperty("service.name") + "] " + statusCode + " : " + httpResponse.getStatusLine().getReasonPhrase());

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
            }

            if (inputResourceID != null) {

                HttpDelete httpDelete = new HttpDelete(config.getProperty("engine.dswarm.api") + "resources/" + inputResourceID);

                CloseableHttpResponse httpResponse = httpclient.execute(httpDelete);

                logger.info("[" + config.getProperty("service.name") + "] " + "request : " + httpDelete.getRequestLine());

                try {

                    int statusCode = httpResponse.getStatusLine().getStatusCode();
                    HttpEntity httpEntity = httpResponse.getEntity();

                    switch (statusCode) {

                        case 200: {

                            logger.info("[" + config.getProperty("service.name") + "] " + statusCode + " : " + httpResponse.getStatusLine().getReasonPhrase());

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
            }

        } finally {
            httpclient.close();
        }
    }

    /**
     * configuration and processing of the task
     *
     * @param inputDataModelID
     * @param projectID
     * @param outputDataModelID
     * @return
     */
    private String executeTask(String inputDataModelID, String projectID, String resourceID, String outputDataModelID) throws Exception {

        String jsonResponse = null;

        CloseableHttpClient httpclient = HttpClients.createDefault();

        try {

            // Hole Mappings aus dem Projekt mit 'projectID'
            HttpGet httpGet = new HttpGet(config.getProperty("engine.dswarm.api") + "projects/" + projectID);

            CloseableHttpResponse httpResponse = httpclient.execute(httpGet);

            logger.info("[" + config.getProperty("service.name") + "] " + "request : " + httpGet.getRequestLine());

            String mappings = "";

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

                        mappings = jsonObject.getJsonArray("mappings").toString();

                        logger.info("[" + config.getProperty("service.name") + "] mappings : " + mappings);

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

            // Hole InputDataModel
            String inputDataModel = "";

            httpGet = new HttpGet(config.getProperty("engine.dswarm.api") + "datamodels/" + inputDataModelID);

            httpResponse = httpclient.execute(httpGet);

            logger.info("[" + config.getProperty("service.name") + "] " + "request : " + httpGet.getRequestLine());

            try {

                int statusCode = httpResponse.getStatusLine().getStatusCode();
                HttpEntity httpEntity = httpResponse.getEntity();

                switch (statusCode) {

                    case 200: {

                        StringWriter writer = new StringWriter();
                        IOUtils.copy(httpEntity.getContent(), writer, "UTF-8");
                        inputDataModel = writer.toString();

                        logger.info("[" + config.getProperty("service.name") + "] inputDataModel : " + inputDataModel);

                        JsonReader jsonReader = Json.createReader(IOUtils.toInputStream(inputDataModel, "UTF-8"));
                        JsonObject jsonObject = jsonReader.readObject();

                        String inputResourceID = jsonObject.getJsonObject("data_resource").getString("uuid");

                        mappings = mappings.replaceAll(resourceID, inputResourceID);

                        logger.info("[" + config.getProperty("service.name") + "] mappings : " + mappings);

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

            // Hole OutputDataModel
            String outputDataModel = "";

            httpGet = new HttpGet(config.getProperty("engine.dswarm.api") + "datamodels/" + outputDataModelID);

            httpResponse = httpclient.execute(httpGet);

            logger.info("[" + config.getProperty("service.name") + "] " + "request : " + httpGet.getRequestLine());

            try {

                int statusCode = httpResponse.getStatusLine().getStatusCode();
                HttpEntity httpEntity = httpResponse.getEntity();

                switch (statusCode) {

                    case 200: {

                        StringWriter writer = new StringWriter();
                        IOUtils.copy(httpEntity.getContent(), writer, "UTF-8");
                        outputDataModel = writer.toString();

                        logger.info("[" + config.getProperty("service.name") + "] outputDataModel : " + outputDataModel);

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

            // erzeuge Task-JSON
            String task = "{";
            task += "\"name\":\"" + "Task Batch-Prozess 'CrossRef'" + "\",";
            task += "\"description\":\"" + "Task Batch-Prozess 'CrossRef' zum InputDataModel '" + inputDataModelID + "'\",";
            task += "\"job\": { " +
                    "\"mappings\": " + mappings + "," +
                    "\"uuid\": \"" + UUID.randomUUID() + "\"" +
                    " },";
            task += "\"input_data_model\":" + inputDataModel + ",";
            task += "\"output_data_model\":" + outputDataModel;
            task += "}";

            logger.info("[" + config.getProperty("service.name") + "] task : " + task);

            // POST http://129.217.132.83:8080/dmp/tasks/
            HttpPost httpPost = new HttpPost(config.getProperty("engine.dswarm.api") + "tasks?persist=" + config.getProperty("results.persistInDMP"));
            StringEntity stringEntity = new StringEntity(task, ContentType.create("application/json", Consts.UTF_8));
            httpPost.setEntity(stringEntity);

            logger.info("[" + config.getProperty("service.name") + "] " + "request : " + httpPost.getRequestLine());

            httpResponse = httpclient.execute(httpPost);

            try {

                int statusCode = httpResponse.getStatusLine().getStatusCode();
                HttpEntity httpEntity = httpResponse.getEntity();

                switch (statusCode) {

                    case 200: {

                        logger.info("[" + config.getProperty("service.name") + "] " + statusCode + " : " + httpResponse.getStatusLine().getReasonPhrase());

                        StringWriter writer = new StringWriter();
                        IOUtils.copy(httpEntity.getContent(), writer, "UTF-8");
                        jsonResponse = writer.toString();

                        logger.info("[" + config.getProperty("service.name") + "] jsonResponse : " + jsonResponse);

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

        return jsonResponse;
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
