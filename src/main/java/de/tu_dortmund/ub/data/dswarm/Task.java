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
//        String updateResourceID = null; try {updateResourceID = getProjectResourceID(dataModelID);} catch (Exception e1) {e1.printStackTrace();}

        // init process values
        String inputResourceID = null;
        String resourceID = null;
        String inputDataModelID = null;
        String message = null;

        try {
            // build a InputDataModel for the resource
            String inputResourceJson = uploadFileToDSwarm(resource, "resource for project '" + resource, config.getProperty("project.name") + "' - case " + cnt);
//            String inputResourceJson = uploadFileToDSwarmAndUpdate(updateResourceID, resource, "resource for project '" + resource, config.getProperty("project.name") + "' - case " + cnt);
            JsonReader jsonReader = Json.createReader(IOUtils.toInputStream(inputResourceJson, "UTF-8"));
            inputResourceID = jsonReader.readObject().getString("uuid");
            logger.info("[" + config.getProperty("service.name") + "] inputResourceID = " + inputResourceID);

            if (inputResourceID != null) {

                // get the resource id of the resource for the data model for the the prototype project
                resourceID = getProjectResourceID(dataModelID);

                if (resourceID != null) {
                    // get the configurations of the prototype resource and build a data model for the new resource
                    inputDataModelID = configureDataModel(inputResourceJson, resourceID, "resource for project '" + resource, config.getProperty("project.name") + "' - case " + cnt);

                    if (inputDataModelID != null) {
                        // configuration and processing of the task
                        String jsonResponse = executeTask(inputDataModelID, projectID, resourceID, outputDataModelID);

                        if (jsonResponse != null) {

                            if (Boolean.parseBoolean(config.getProperty("results.persistInFolder"))) {

                                // save results in files
                                FileUtils.writeStringToFile(new File(config.getProperty("results.folder") + File.separatorChar + inputDataModelID + ".json"), jsonResponse);

                                message = "'" + resource + "' transformed. results in '" + config.getProperty("results.folder") + File.separatorChar + inputDataModelID + ".json" + "'";
                            }
                        } else {

                            message = "'" + resource + "' not transformed: error in task execution.";
                        }
                    }
                }
            }

            // cleanup data model and resource in d:swarm
//            cleanup(inputResourceID, inputDataModelID);
            cleanup(null, inputDataModelID); // don't clean up our updateable-Resource
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
     * get the configurations of the prototype resource and build a data model for the new resource
     *
     * @param inputResourceJson
     * @param resourceID
     * @param name
     * @param description
     * @return
     * @throws Exception
     */
    private String configureDataModel(String inputResourceJson, String resourceID, String name, String description) throws Exception {

        String inputDataModelID = null;

        JsonReader jsonReader = Json.createReader(IOUtils.toInputStream(inputResourceJson, "UTF-8"));
        String inputResourceID = jsonReader.readObject().getString("uuid");

        CloseableHttpClient httpclient = HttpClients.createDefault();

        // Hole die Konfiguration zur Prototyp-Projekt
        try {

            HttpGet httpGet = new HttpGet(config.getProperty("engine.dswarm.api") + "resources/" + resourceID + "/configurations");

            CloseableHttpResponse httpResponse = httpclient.execute(httpGet);

            logger.info("[" + config.getProperty("service.name") + "] " + "request : " + httpGet.getRequestLine());

            String json = "";

            try {

                int statusCode = httpResponse.getStatusLine().getStatusCode();
                HttpEntity httpEntity = httpResponse.getEntity();

                switch (statusCode) {

                    case 200: {

                        StringWriter writer = new StringWriter();
                        IOUtils.copy(httpEntity.getContent(), writer, "UTF-8");
                        String responseJson = writer.toString();

                        logger.info("[" + config.getProperty("service.name") + "] responseJson : " + responseJson);

                        // replace resourceID with inputResourceID
                        responseJson = responseJson.substring(1, responseJson.length() - 1);

                        jsonReader = Json.createReader(IOUtils.toInputStream(responseJson, "UTF-8"));
                        JsonObject jsonObject = jsonReader.readObject();

                        String nameParam = "unnamed";
                        String descriptionParam = "unnamed";
                        try {
                        	nameParam = jsonObject.getString("name");
                        	descriptionParam = jsonObject.getString("description");
                        } 
                        catch (ClassCastException e) {}
                        logger.info("[" + config.getProperty("service.name") + "] nameParam : " + nameParam);
                    	logger.info("[" + config.getProperty("service.name") + "] descriptionParam : " + descriptionParam);
                        String resourcesParam = jsonObject.getJsonArray("resources").toString();
                        logger.info("[" + config.getProperty("service.name") + "] resourcesParam : " + resourcesParam);
                        String parametersParam = jsonObject.getJsonObject("parameters").toString();
                        logger.info("[" + config.getProperty("service.name") + "] parametersParam : " + parametersParam);

                        json = "{";
                        json += "\"uuid\":\"" + UUID.randomUUID() + "\",";
                        json += "\"name\":\"" + nameParam + "\",";
                        json += "\"description\":\"" + descriptionParam.replaceAll(resourceID,inputResourceID) + "\",";
                        json += "\"resources\":" + resourcesParam.replaceAll(resourceID,inputResourceID) + ",";
                        json += "\"parameters\":" + parametersParam;
                        json += "}";

                        logger.info("[" + config.getProperty("service.name") + "] json : " + json);

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

            if (!json.equals("")) {

                // Konfiguration der neuen Resource: http://129.217.132.83:8080/dmp/resources/{uuid der neuen Ressource}/configurations
                HttpPost httpPost = new HttpPost(config.getProperty("engine.dswarm.api") + "resources/" + inputResourceID + "/configurations");
                StringEntity stringEntity = new StringEntity(json, ContentType.create("application/json", Consts.UTF_8));
                httpPost.setEntity(stringEntity);

                logger.info("[" + config.getProperty("service.name") + "] " + "request : " + httpPost.getRequestLine());

                httpResponse = httpclient.execute(httpPost);

                try {

                    int statusCode = httpResponse.getStatusLine().getStatusCode();
                    HttpEntity httpEntity = httpResponse.getEntity();

                    switch (statusCode) {

                        case 201: {

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

                // Anlegen eines neuen Data Model: http://129.217.132.83:8080/dmp/datamodels
                httpPost = new HttpPost(config.getProperty("engine.dswarm.api") + "datamodels");

                String datamodel = "{ " +
                        "\"name\" : \""+ name + "\", " +
                        "\"description\" : \"" + description + "\", " +
                        "\"configuration\" : " + json + ", " +
                        "\"data_resource\" : " + inputResourceJson.replace("\"configuration\" : []", "\"configuration\" : [" + json + "]") +
                        " }";

                logger.info("[" + config.getProperty("service.name") + "] datamodel : " + datamodel);

                stringEntity = new StringEntity(datamodel, ContentType.create("application/json", Consts.UTF_8));
                httpPost.setEntity(stringEntity);

                logger.info("[" + config.getProperty("service.name") + "] " + "request : " + httpPost.getRequestLine());

                httpResponse = httpclient.execute(httpPost);

                try {

                    int statusCode = httpResponse.getStatusLine().getStatusCode();
                    HttpEntity httpEntity = httpResponse.getEntity();

                    switch (statusCode) {

                        case 201: {

                            logger.info("[" + config.getProperty("service.name") + "] " + statusCode + " : " + httpResponse.getStatusLine().getReasonPhrase());

                            jsonReader = Json.createReader(httpEntity.getContent());
                            inputDataModelID = jsonReader.readObject().getString("uuid");

                            logger.info("[" + config.getProperty("service.name") + "] inputDataModelID = " + inputDataModelID);

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

        return inputDataModelID;
    }

    /**
     * get the resource id of the resource for the data model for the the prototype project
     *
     * @param dataModelID
     * @return
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
     * build a InputDataModel for the resource
     *
     * @param filename
     * @param name
     * @param description
     * @return
     * @throws Exception
     */
    private String uploadFileToDSwarm(String filename, String name, String description) throws Exception {

        String responseJson = null;

        String file = config.getProperty("resource.watchfolder") + File.separatorChar +  filename;

        // ggf. Preprocessing: insert CDATA in XML and write new XML file to tmp folder
        if (Boolean.parseBoolean(config.getProperty("resource.preprocessing"))) {

            Document document = new SAXBuilder().build(new File(file));

            file = config.getProperty("preprocessing.folder") + File.separatorChar + UUID.randomUUID() + ".xml";

            XMLOutputter out = new XMLOutputter(Format.getPrettyFormat());
            BufferedWriter bufferedWriter = null;
            try {

                bufferedWriter = new BufferedWriter(new FileWriter(file));

                out.output(new SAXBuilder().build(new StringReader(XmlTransformer.xmlOutputter(document, config.getProperty("preprocessing.xslt"), null))), bufferedWriter);
            }
            finally {
                if (bufferedWriter != null) {
                    bufferedWriter.close();
                }
            }
        }

        // upload
        CloseableHttpClient httpclient = HttpClients.createDefault();

        try {
            HttpPost httpPost = new HttpPost(config.getProperty("engine.dswarm.api") + "resources/");

            FileBody fileBody = new FileBody(new File(file));
            StringBody stringBodyForName = new StringBody(name, ContentType.TEXT_PLAIN);
            StringBody stringBodyForDescription = new StringBody(description, ContentType.TEXT_PLAIN);

            HttpEntity reqEntity = MultipartEntityBuilder.create()
                    .addPart("file", fileBody)
                    .addPart("name", stringBodyForName)
                    .addPart("description", stringBodyForDescription)
                    .build();

            httpPost.setEntity(reqEntity);

            logger.info("[" + config.getProperty("service.name") + "] " + "request : " + httpPost.getRequestLine());

            CloseableHttpResponse httpResponse = httpclient.execute(httpPost);

            try {
                int statusCode = httpResponse.getStatusLine().getStatusCode();
                HttpEntity httpEntity = httpResponse.getEntity();

                switch (statusCode) {

                    case 201: {

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

        // ggf. Löschen des tmp resource file
        if (Boolean.parseBoolean(config.getProperty("resource.preprocessing"))) {

            File f = new File(file);

            boolean isDeleted = f.delete();

            logger.info("[" + config.getProperty("service.name") + "] tmp file '" + file + "' deleted? " + isDeleted);
        }

        return responseJson;
    }
    
    /**
     * build a InputDataModel for the resource
     *
     * @param filename
     * @param name
     * @param description
     * @return
     * @throws Exception
     */
    private String uploadFileToDSwarmAndUpdate(String resourceUUID, String filename, String name, String description) throws Exception { //

    	if (null == resourceUUID) throw new Exception("ID of the resource to update was null.");
    	
        String responseJson = null;

        String file = config.getProperty("resource.watchfolder") + File.separatorChar +  filename;

        // upload
        CloseableHttpClient httpclient = HttpClients.createDefault();

        try {
            HttpPut httpPut = new HttpPut(config.getProperty("engine.dswarm.api") + "resources/" + resourceUUID); //

            FileBody fileBody = new FileBody(new File(file));
            StringBody stringBodyForName = new StringBody(name, ContentType.TEXT_PLAIN);
            StringBody stringBodyForDescription = new StringBody(description, ContentType.TEXT_PLAIN);

            HttpEntity reqEntity = MultipartEntityBuilder.create()
                    .addPart("file", fileBody)
                    .addPart("name", stringBodyForName)
                    .addPart("description", stringBodyForDescription)
                    .build();

            httpPut.setEntity(reqEntity);//

            logger.info("[" + config.getProperty("service.name") + "] " + "request : " + httpPut.getRequestLine());//

            CloseableHttpResponse httpResponse = httpclient.execute(httpPut);//

            try {
                int statusCode = httpResponse.getStatusLine().getStatusCode();
                HttpEntity httpEntity = httpResponse.getEntity();

                switch (statusCode) {

                    case 201: {

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
