package com.capgemini.spark.input;

import com.capgemini.commons.ingestionRequest;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.Map;

public interface InputSource<T> {
    void createDataset(ingestionRequest ir, SparkSession spark)
        throws Exception;
    String createDatasetTempTable( Dataset<T> df,String tmpTableName)
            throws Exception;

}
