package com.capgemini.commons;

public class dsQueryRequest {
    private String dsQueryName;
    private String dataFrameNam;
    private String rawSql;

    public String getDsQueryName() {
        return dsQueryName;
    }

    public void setDsQueryName(String dsQueryName) {
        this.dsQueryName = dsQueryName;
    }

    public String getDataFrameNam() {
        return dataFrameNam;
    }

    public void setDataFrameNam(String dataFrameNam) {
        this.dataFrameNam = dataFrameNam;
    }

    public String getRawSql() {
        return rawSql;
    }

    public void setRawSql(String rawSql) {
        this.rawSql = rawSql;
    }
}
