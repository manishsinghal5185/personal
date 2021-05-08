package com.capgemini.commons;

public class dsQueryRequest {
    private String dsQueryName;
    private String dataFrameName;
    private String rawSql;

    public String getDsQueryName() {
        return dsQueryName;
    }

    public void setDsQueryName(String dsQueryName) {
        this.dsQueryName = dsQueryName;
    }

    public String getDataFrameName() {
        return dataFrameName;
    }

    public void setDataFrameName(String dataFrameNam) {
        this.dataFrameName = dataFrameNam;
    }

    public String getRawSql() {
        return rawSql;
    }

    public void setRawSql(String rawSql) {
        this.rawSql = rawSql;
    }
}
