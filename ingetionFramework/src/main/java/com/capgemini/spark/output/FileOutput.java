package com.capgemini.spark.output;

import com.capgemini.commons.MetaConfigConst;
import com.capgemini.commons.dataTarget;
import com.capgemini.commons.ingestionRequest;
import org.apache.ivy.util.StringUtils;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.stream.Collectors;

public class FileOutput implements SparkExport{
    private static final Logger log= LoggerFactory.getLogger(FileOutput.class);
    private dataTarget target;
    public FileOutput(dataTarget target){this.target=target;}
    @Override
    public void writeDataset(ingestionRequest ir, SparkSession spark){
        String targetLoc=target.getStagingLocation();
        String dataFrameName=target.getDataFrameName();
        Dataset<Row> df=spark.table(dataFrameName);
        DataFrameWriter<Row> dfw=df.write();
        if(target.getOverwriteFlag()!=null)
            dfw.mode(SaveMode.Overwrite);
        if (MetaConfigConst.CSV.equalsIgnoreCase(target.getResultFormat())) {
            if (target.isHeaders())
                dfw.option("header", "true");
            dfw.csv(targetLoc);
        }
        if(target.getPartition()!=null && target.getPartition().length>0) {
            log.info("Partitioning data using:{}", StringUtils.join(target.getPartition(),","));
            dfw.partitionBy(target.getPartition());
        }
        if (MetaConfigConst.PARQUET.equalsIgnoreCase(target.getResultFormat()))
            dfw.parquet(targetLoc);
//        if (MetaConfigConst.AVRO.equalsIgnoreCase(target.getResultFormat()))
//            dfw.format("avro").save(targetLoc);
        createExternalTable(df,spark);

    }
    void createExternalTable(Dataset<Row> df,SparkSession spark){
        spark.sql("set hive.exec.dynamic.partition.mode=nonstrict");
        String[] partCols=target.getPartition()==null? new String[]{""} :target.getPartition();
        StructType schema=df.schema();
        String columns= Arrays.stream(schema.fields())
                .filter(field->!Arrays.asList(partCols).contains(field.name()))
                .map(field->field.name()+" "+field.dataType().simpleString())
                .collect(Collectors.joining(","));
        log.info("List of columns names:{}",columns);

        String partColumns= Arrays.stream(schema.fields())
                .filter(field->Arrays.asList(partCols).contains(field.name()))
                .map(field->field.name()+" "+field.dataType().simpleString())
                .collect(Collectors.joining(","));
        log.info("List of parition columns names:{}",partColumns);
     if (target.getPartition()!=null)  {
         String createSql="create external table if not exists "+target.getTargetTableName()
                 +" ("
                 +columns
                 +") partitioned by ("
                 +partColumns
                 +") stored as parquet location '"
                 +target.getStagingLocation()
                 +"'";
         log.info("Create table Statement is:{}",createSql);
         spark.sql(createSql);
     }else {
         String createSql="create external table if not exists "+target.getTargetTableName()
                 +" ("
                 +columns
                 +") stored as parquet location '"
                 +target.getStagingLocation()
                 +"'";
         log.info("Create table Statement is:{}",createSql);
         spark.sql(createSql);
     }
    }
}
