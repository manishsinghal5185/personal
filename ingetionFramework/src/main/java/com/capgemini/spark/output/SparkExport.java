package com.capgemini.spark.output;

import com.capgemini.commons.ingestionRequest;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public interface SparkExport {
    public void writeDataset(ingestionRequest ir, SparkSession spark)
            throws Exception;
}
