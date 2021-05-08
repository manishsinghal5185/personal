package com.capgemini.commons;

public class dsFileRequest {
    String dsFileName;
    String dataFrameName;
    String dsSchemaFileName;
    String dsFileType;
    boolean isHeader;
    String dsFileDelimiter;
    dsControlFileRequest dsControlFile ;

    public String getDsFileName() {
        return dsFileName;
    }

    public void setDsFileName(String dsFileName) {
        this.dsFileName = dsFileName;
    }

    public String getDataFrameName() {
        return dataFrameName;
    }

    public void setDataFrameName(String dataFrameName) {
        this.dataFrameName = dataFrameName;
    }

    public String getDsSchemaFileName() {
        return dsSchemaFileName;
    }

    public void setDsSchemaFileName(String dsSchemaFileName) {
        this.dsSchemaFileName = dsSchemaFileName;
    }

    public String getDsFileType() {
        return dsFileType;
    }

    public void setDsFileType(String dsFileType) {
        this.dsFileType = dsFileType;
    }

    public boolean isHeader() {
        return isHeader;
    }

    public void setHeader(boolean header) {
        isHeader = header;
    }

    public String getDsFileDelimiter() {
        return dsFileDelimiter;
    }

    public void setDsFileDelimiter(String dsFileDelimiter) {
        this.dsFileDelimiter = dsFileDelimiter;
    }

    public dsControlFileRequest getDsControlFile() {
        return dsControlFile;
    }

    public void setDsControlFile(dsControlFileRequest dsControlFile) {
        this.dsControlFile = dsControlFile;
    }
}
