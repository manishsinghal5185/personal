package com.capgemini.commons;

public class dsDdbRequest {
    String dbQuery;
    String dataFrameName;
    String sourceDBType;
    String connectionString;
    Integer batchSize;

    public String getDbQuery() {
        return dbQuery;
    }

    public void setDbQuery(String dbQuery) {
        this.dbQuery = dbQuery;
    }

    public String getDataFrameName() {
        return dataFrameName;
    }

    public void setDataFrameName(String dataFrameName) {
        this.dataFrameName = dataFrameName;
    }

    public String getSourceDBType() {
        return sourceDBType;
    }

    public void setSourceDBType(String sourceDBType) {
        this.sourceDBType = sourceDBType;
    }

    public String getConnectionString() {
        return connectionString;
    }

    public void setConnectionString(String connectionString) {
        this.connectionString = connectionString;
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(Integer batchSize) {
        this.batchSize = batchSize;
    }
}
