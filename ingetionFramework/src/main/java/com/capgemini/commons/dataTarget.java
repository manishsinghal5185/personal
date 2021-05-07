package com.capgemini.commons;

import java.util.List;

public class dataTarget {
    private String targetName;
    private String dataFrameName;
    private String resultFormat;
    private String targetTableName;
    private boolean headers;
    private Boolean overwriteFlag;
    private String stagingLocation;
    private String[] partition;

    public String getTargetName() {
        return targetName;
    }

    public void setTargetName(String targetName) {
        this.targetName = targetName;
    }

    public String getDataFrameName() {
        return dataFrameName;
    }

    public void setDataFrameName(String dataFrameName) {
        this.dataFrameName = dataFrameName;
    }

    public String getResultFormat() {
        return resultFormat;
    }

    public void setResultFormat(String resultFormat) {
        this.resultFormat = resultFormat;
    }

    public String getTargetTableName() {
        return targetTableName;
    }

    public void setTargetTableName(String targetTableName) {
        this.targetTableName = targetTableName;
    }

    public boolean isHeaders() {
        return headers;
    }

    public void setHeaders(boolean headers) {
        this.headers = headers;
    }

    public Boolean getOverwriteFlag() {
        return overwriteFlag;
    }

    public void setOverwriteFlag(Boolean overwriteFlag) {
        this.overwriteFlag = overwriteFlag;
    }

    public String getStagingLocation() {
        return stagingLocation;
    }

    public void setStagingLocation(String stagingLocation) {
        this.stagingLocation = stagingLocation;
    }

    public String[] getPartition() {
        return partition;
    }

    public void setPartition(String[] partition) {
        this.partition = partition;
    }
}
