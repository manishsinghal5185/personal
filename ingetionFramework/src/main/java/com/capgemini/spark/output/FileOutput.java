package com.capgemini.spark.output;

import com.capgemini.commons.MetaConfigConst;
import com.capgemini.commons.dataTarget;
import com.capgemini.commons.ingestionRequest;
import com.capgemini.spark.input.FileInputSource;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Map;
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
        if(target.getPartiton()!=null &&!target.getPartiton().isEmpty()) {
            String parKey = target.getPartiton().stream().map(n -> String.valueOf(n)).collect(Collectors.joining(","));
            log.info("Partitioning data using:{}",parKey);
            dfw.partitionBy(parKey);
        }
        if (MetaConfigConst.PARQUET.equalsIgnoreCase(target.getResultFormat()))
            dfw.parquet(targetLoc);


    }
}
