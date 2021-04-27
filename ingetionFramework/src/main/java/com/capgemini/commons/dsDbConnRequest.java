package com.capgemini.commons;

public class dsDbConnRequest {
    String sourceDBType;
    String connectionString;
    String userLogin;

    public String getUserLogin() {
        return userLogin;
    }

    public void setUserLogin(String userLogin) {
        this.userLogin = userLogin;
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
}
