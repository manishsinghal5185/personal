package com.capgemini.commons;

public class MetaConfigConst {
    public static final String appName="appName";
    public static final String sor="sorName";
    public static final String appId="appId";
    public static final String subjectArea="subjectArea";
    public static final String dataSource="dataSource";
    public static final String dsDB="dsDB";
    public static final String dsDBs="dsDBs";
    public static final String dsDBConn="dsDBConn";
    public static final String userLogin="userLogin";
    public static final String dbQuery="dbQuery";
    public static final String dataFrameName="dataFrameName";
    public static final String sourceDBType="sourceDBType";
    public static final String connectionString="connectionString";
    public static final String batchSize="batchSize";
    public static final String dsFiles="dsFiles";
    public static final String dsFile="dsFile";
    public static final String dsFileName="dsFileName";
    public static final String dsSchemaFileName="dsSchemaFileName";
    public static final String dsFileType="dsFileType";
    public static final String isHeader="isHeader";
    public static final String dsFileDelimiter="dsFileDelimiter";
    public static final String dsControlFile="dsControlFile";
    public static final String dsQueries="dsQueries";
    public static final String dsQuery="dsQuery";
    public static final String dsQueryName="dsQueryName";
    public static final String rawSql="rawSql";
    public static final String staging="staging";
    public static final String dataTarget="dataTarget";
    public static final String dataTargets="dataTargets";
    public static final String targetName="targetName";
    public static final String resutFormat="resutFormat";
    public static final String targetTableName="targetTableName";
    public static final String headers="headers";
    public static final String overwriteFlag="overwriteFlag";
    public static final String stagingLocation="stagingLocation";
    public static final String partitons="partitons";
    public static final String sparkConfigParams="sparkConfigParams";
    public static final String sparkConfigName="sparkConfigName";
    public static final String sparkConfigValue="sparkConfigValue";
    public static final String sparkConfigType="sparkConfigType";
    public static final String CSV="CSV";
    public static final String XML="XML";
    public static final String JSON="JSON";
    public static final String PARQUET="PARQUET";
    public static final String AVRO="AVRO";
    public static enum FILE_TYPE{
        CSV(1),FIXEDWIDTH(2),EDCDIC(3),JSON(3),XML(4);
        private final int code;
        FILE_TYPE(int code) {
            this.code = code;
        }
    }
}
