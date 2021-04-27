package com.capgemini.commons;

public class dsDbRequest {
    String dbQuery;
    String dataFrameName;

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

    public Integer getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(Integer batchSize) {
        this.batchSize = batchSize;
    }
}
