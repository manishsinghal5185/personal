package com.capgemini;
import com.capgemini.spark.output.FileOutput;
import com.capgemini.spark.output.SparkExport;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.capgemini.commons.*;
import com.capgemini.spark.input.*;
import org.apache.commons.cli.*;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class ingestionServiceRunner {
    private static final String PARAM_DELIM=",";
    private static final String PARAM_KV_DELIM="=";
    protected static final Logger log=LoggerFactory.getLogger(ingestionServiceRunner.class);

    public static void main(String[] args) throws Exception {
        log.info("command line arguments: {}", Arrays.deepToString(args));
        CommandLine commandLine=commandLineParse(args);
        String paramStr=commandLine.getOptionValue('p');
        String envStr=commandLine.getOptionValue('e');
        String configFileName=commandLine.getOptionValue('f');
        log.info("==========ENV:{}",envStr);
        log.info("JSON configuration passed is:{}",configFileName);
        log.info("parameters passed is:{}",paramStr);
        ObjectMapper m= new ObjectMapper();
        JsonNode configNode=readFromFiles(configFileName);
        ingestionRequest IR= new ingestionRequest();
        buildIngestionRequest(IR,configNode,parseParameters(paramStr));
        System.out.println("ingestionRequestObject is:"+m.writerWithDefaultPrettyPrinter().writeValueAsString(IR));
        //new readFiles().reader(IR);
//        String sparkAppName=IR.getAppName();
//        SparkSession spark=SparkSession
//                .builder()
//                .appName(sparkAppName)
//                .enableHiveSupport()
//                .config("spark.sql.hive.convertMetastoreParquet",false)
//                .getOrCreate();
//        for(dsFileRequest dsfile:IR.getDsFiles()) {
//            InputSource<Row> is = new FileInputSource(dsfile);
//            is.createDataset(IR,spark);
//        }
//
//        for(dataTarget dt: IR.getDataTargets()){
//            SparkExport out=new FileOutput(dt);
//            out.writeDataset(IR,spark);
//        }

    }
    public static CommandLine commandLineParse(String[] args) throws ParseException {
        Options options = new Options().addOption("f", "configFile",true,"Json Configuration Files")
                .addOption("p","parameters",true,"parameters")
                .addOption("e","env",true,"running enviornment");
        return new BasicParser().parse(options,args);

    }
    protected static List<Map<String,String>> parseParameters(String parameters){
        List<Map<String,String>> ret=new ArrayList<>();
        if (null!=parameters){
            String[] parList=parameters.split(PARAM_DELIM);
            for (String onePar:parList) {
                String oneParamTrim=onePar.trim();
                Map<String, String> oneParam=new HashMap<>();
                String key = "";
                String value = null;
                int pos=oneParamTrim.indexOf(PARAM_KV_DELIM);
                if (pos>=0){
                    key=oneParamTrim.substring(0,pos);
                    value=oneParamTrim.substring(pos+1);
                }else {
                    key=oneParamTrim;
                }
                oneParam.put(key,value);
                ret.add(oneParam);
            }

        }
        return ret;
    }

    public static void buildIngestionRequest(ingestionRequest ir, JsonNode configNode,List<Map<String,String>> parameters) {
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
        //set parameters
        ir.setCmdLineParameters(parameters);
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
                JsonNode fileTypeNode=dsFileNode.path(MetaConfigConst.dsFileType);
                if (!fileTypeNode.isMissingNode()) {
                    dsFile.setDsFileType(dsFileNode.path(MetaConfigConst.dsFileType).asText());
                }else {
                    dsFile.setDsFileName(MetaConfigConst.FILE_TYPE.CSV.toString());
                }
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
                //set staging loc
                String targetTableName=targetNode.path(MetaConfigConst.targetTableName).asText().trim();
                String stagingLoc=targetNode.path(MetaConfigConst.stagingLocation).asText().trim().replaceAll("([^/])$","$1/")+targetTableName;
                target.setStagingLocation(stagingLoc);
                target.setTargetName(targetNode.path(MetaConfigConst.targetName).asText());
                target.setTargetTableName(targetTableName);
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
