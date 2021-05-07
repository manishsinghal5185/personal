package com.capgemini.spark.input;

import com.capgemini.commons.dsQueryRequest;
import com.capgemini.commons.ingestionRequest;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class queryInputSource implements InputSource<Row> {
    private static final Logger log= LoggerFactory.getLogger(FileInputSource.class);
    private List<dsQueryRequest> dsQueries;
    @Override
    public void createDataset(ingestionRequest ir, SparkSession spark) throws Exception {
    dsQueries=ir.getDsQueries();
    Dataset<Row> df;
    for (dsQueryRequest dsQuery:dsQueries){
        String rawSql=dsQuery.getRawSql();
        String tmpTable=dsQuery.getDataFrameName();
        df=spark.sql(rawSql);
        createDatasetTempTable(df,tmpTable);
    }
    }

    @Override
    public String createDatasetTempTable(Dataset<Row> df,String tmpTableName) throws Exception {
        log.info("creating dataFrame as:{}",tmpTableName);
        if (tmpTableName!=null)
            df.createOrReplaceTempView(tmpTableName);
        return tmpTableName;
    }
}
