package com.capgemini;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.capgemini.commons.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ingestionServiceRunner {
    public static void main(String[] args) throws IOException {
        String configFileName="C:\\Users\\msingha1\\OneDrive - Capgemini\\Documents\\I&D Practice\\template.json";
        ObjectMapper m= new ObjectMapper();
        JsonNode configNode=readFromFiles(configFileName);
        //System.out.println(configNode.toPrettyString());
        ingestionRequest IR= new ingestionRequest();
        buildIngestionRequest(IR,configNode);
        System.out.println(m.writerWithDefaultPrettyPrinter().writeValueAsString(IR));

    }

    public static void buildIngestionRequest(ingestionRequest ir, JsonNode configNode) {
        String appName=configNode.path(MetaConfigConst.appName).asText();
        String sor=configNode.path(MetaConfigConst.sor).asText();
        String appId=configNode.path(MetaConfigConst.appId).asText();
        String subjectArea=configNode.path(MetaConfigConst.subjectArea).asText();
        ir.setAppName(appName);
        ir.setAppId(appId);
        ir.setSor(sor);
        ir.setSubjectArea(subjectArea);
        JsonNode dataSourceNode=configNode.path(MetaConfigConst.dataSource);
        JsonNode dsDbConnNode=dataSourceNode.path(MetaConfigConst.dsDBConn);
        JsonNode dsDBsNode =dataSourceNode.path(MetaConfigConst.dsDBs);
        JsonNode dsFilesNode=dataSourceNode.path(MetaConfigConst.dsFiles);
        JsonNode dsQueriesNode=dataSourceNode.path(MetaConfigConst.dsQueries);
        JsonNode stagingNode=configNode.path(MetaConfigConst.staging);
        JsonNode dataTargetNode=configNode.path(MetaConfigConst.dataTargets);
        JsonNode sparkConfigParamsNode=configNode.path(MetaConfigConst.sparkConfigParams);
        //dsDBConnection
        if (!dsDbConnNode.isMissingNode()){
            dsDbConnRequest dsDBConn=new dsDbConnRequest();
            dsDBConn.setConnectionString(dsDbConnNode.path(MetaConfigConst.connectionString).asText());
            dsDBConn.setSourceDBType(dsDbConnNode.path(MetaConfigConst.sourceDBType).asText());
            dsDBConn.setUserLogin(dsDbConnNode.path(MetaConfigConst.userLogin).asText());
            ir.setDsDbConn(dsDBConn);
        }
        //dsDBs
        if(!dsDBsNode.isMissingNode()){
            List <dsDbRequest> dsDbRequests =new ArrayList<>();
            for(JsonNode dsDBNodeObj: dsDBsNode){
                JsonNode dsDBNode=dsDBNodeObj.path(MetaConfigConst.dsDB);
                dsDbRequest dsDB=new dsDbRequest();
                dsDB.setBatchSize(dsDBNode.path(MetaConfigConst.batchSize).asInt());

                dsDB.setDataFrameName(dsDBNode.path(MetaConfigConst.dataFrameName).asText());
                dsDB.setDbQuery(dsDBNode.path(MetaConfigConst.dbQuery).asText());

                dsDbRequests.add(dsDB);
            }
            ir.setDsDBs(dsDbRequests);
        }
        //dsFiles
        if(!dsFilesNode.isMissingNode()){
            List <dsFileRequest> dsFileRequests=new ArrayList<>();
            for (JsonNode dsFileNodeObj:dsFilesNode){
                JsonNode dsFileNode=dsFileNodeObj.path(MetaConfigConst.dsFile);
                dsFileRequest dsFile=new dsFileRequest();
                //dsControlFileRequest dsControlFile=new dsControlFileRequest();
                dsFile.setDataFrameName(dsFileNode.path(MetaConfigConst.dataFrameName).asText());
                //write code for dsControlFile
                //dsFile.setDsControlFile(dsControlFile);
                dsFile.setDsFileDelimiter(dsFileNode.path(MetaConfigConst.dsFileDelimiter).asText());
                dsFile.setDsFileName(dsFileNode.path(MetaConfigConst.dsFileName).asText());
                dsFile.setDsFileType(dsFileNode.path(MetaConfigConst.dsFileType).asText());
                dsFile.setDsSchemaFileName(dsFileNode.path(MetaConfigConst.dsSchemaFileName).asText());
                dsFile.setHeader(dsFileNode.path(MetaConfigConst.headers).asBoolean());
                dsFileRequests.add(dsFile);
            }
            ir.setDsFiles(dsFileRequests);
        }
        //dsQueries
        if (!dsQueriesNode.isMissingNode()){
            List <dsQueryRequest> dsQueryRequests=new ArrayList<>();
            for (JsonNode dsQueryNodeObj:dsQueriesNode){
                JsonNode dsQueryNode=dsQueryNodeObj.path(MetaConfigConst.dbQuery);
                dsQueryRequest dsQuery=new dsQueryRequest();
                dsQuery.setDataFrameNam(dsQueryNode.path(MetaConfigConst.dataFrameName).asText());
                dsQuery.setDsQueryName(dsQueryNode.path(MetaConfigConst.dsQueryName).asText());
                dsQuery.setRawSql(dsQueryNode.path(MetaConfigConst.rawSql).asText());
                dsQueryRequests.add(dsQuery);
            }
            ir.setDsQueries(dsQueryRequests);
        }
        //staging
        ir.setStagingLoc(stagingNode.path(MetaConfigConst.stagingLocation).asText());
        JsonNode partitonNode=stagingNode.path(MetaConfigConst.partitons);
        if(!partitonNode.isMissingNode() && partitonNode.isArray() && partitonNode.size()>0){
            List<String> patitions=new ArrayList<>();
            for (JsonNode par:partitonNode){
                patitions.add(par.asText());
            }
            ir.setStagingPartitions(patitions);
        }
        //dstargets
        List<dataTarget> targets=new ArrayList<>();
        if(!dataTargetNode.isMissingNode() && dataTargetNode.isArray() && dataTargetNode.size()>0){
            for (JsonNode targetNode:dataTargetNode){
                dataTarget target=new dataTarget();
                target.setDataFrameName(targetNode.path(MetaConfigConst.dataFrameName).asText());
                target.setHeaders(targetNode.path(MetaConfigConst.headers).asBoolean());
                target.setOverwriteFlag(targetNode.path(MetaConfigConst.overwriteFlag).asBoolean());
                JsonNode tgtPartitonNode=targetNode.path(MetaConfigConst.partitons);
                if(!tgtPartitonNode.isMissingNode() && tgtPartitonNode.isArray() && tgtPartitonNode.size()>0){
                    List<String> patitions=new ArrayList<>();
                    for (JsonNode par:partitonNode){
                        patitions.add(par.asText());
                    }
                   target.setPartiton(patitions);
                }
                target.setResultFormat(targetNode.path(MetaConfigConst.resutFormat).asText());
                target.setStagingLocation(targetNode.path(MetaConfigConst.stagingLocation).asText());
                target.setTargetName(targetNode.path(MetaConfigConst.targetName).asText());
                target.setTargetTableName(targetNode.path(MetaConfigConst.targetTableName).asText());
                targets.add(target);
            }
            ir.setDataTargets(targets);

        }
        //spark confit params
        List<sparkConfigParam> sparkConfigParams=new ArrayList<>();
        if (!sparkConfigParamsNode.isMissingNode()){
            for(JsonNode item:sparkConfigParamsNode){
                sparkConfigParam sp=new sparkConfigParam();
                sp.setSparkConfigName(item.path(MetaConfigConst.sparkConfigName).asText());
                sp.setSparkConfigType(item.path(MetaConfigConst.sparkConfigType).asText());
                sp.setSparkConfigValue(item.path(MetaConfigConst.sparkConfigValue).asText());
                sparkConfigParams.add(sp);
            }
            ir.setSparkConfigParams(sparkConfigParams);
        }

    }

    protected static JsonNode readFromFiles(String configFileName) throws IOException {
        ObjectMapper m= new ObjectMapper();
        File configFile= new File(configFileName);
        JsonNode ret=m.readTree(configFile);
        return ret;

    }
}
