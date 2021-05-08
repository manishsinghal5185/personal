package com.capgemini.commons;

import java.util.List;
import java.util.Map;

public class ingestionRequest {
    private String appName;
    private String sor;
    private String appId;
    private String subjectArea;
    private dsDbConnRequest dsDbConn;
    private List<dsDbRequest> dsDBs;
    private List <dsFileRequest> dsFiles;
    private List <dsQueryRequest> dsQueries;
    private List <dataTarget> dataTargets;
    private String  stagingLoc;
    private List <String> stagingPartitions;
    private List <sparkConfigParam>  sparkConfigParams;
    private List<Map<String,String>> cmdLineParameters;

    public List<sparkConfigParam> getSparkConfigParams() {
        return sparkConfigParams;
    }

    public void setSparkConfigParams(List<sparkConfigParam> sparkConfigParams) {
        this.sparkConfigParams = sparkConfigParams;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getSor() {
        return sor;
    }

    public void setSor(String sor) {
        this.sor = sor;
    }

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public String getSubjectArea() {
        return subjectArea;
    }

    public void setSubjectArea(String subjectArea) {
        this.subjectArea = subjectArea;
    }

    public List<dsDbRequest> getDsDBs() {
        return dsDBs;
    }

    public void setDsDBs(List<dsDbRequest> dsDBs) {
        this.dsDBs = dsDBs;
    }

    public List<dsFileRequest> getDsFiles() {
        return dsFiles;
    }

    public void setDsFiles(List<dsFileRequest> dsFiles) {
        this.dsFiles = dsFiles;
    }

    public List<dsQueryRequest> getDsQueries() {
        return dsQueries;
    }

    public void setDsQueries(List<dsQueryRequest> dsQueries) {
        this.dsQueries = dsQueries;
    }

    public List<dataTarget> getDataTargets() {
        return dataTargets;
    }

    public void setDataTargets(List<dataTarget> dataTargets) {
        this.dataTargets = dataTargets;
    }

    public String getStagingLoc() {
        return stagingLoc;
    }

    public void setStagingLoc(String stagingLoc) {
        this.stagingLoc = stagingLoc;
    }

    public List<String> getStagingPartitions() {
        return stagingPartitions;
    }

    public void setStagingPartitions(List<String> stagingPartitions) {
        this.stagingPartitions = stagingPartitions;
    }


    public dsDbConnRequest getDsDbConn() {
        return dsDbConn;
    }

    public void setDsDbConn(dsDbConnRequest dsDbConn) {
        this.dsDbConn = dsDbConn;
    }

    public List<Map<String, String>> getCmdLineParameters() {
        return cmdLineParameters;
    }

    public void setCmdLineParameters(List<Map<String, String>> cmdLineParameters) {
        this.cmdLineParameters = cmdLineParameters;
    }
}
