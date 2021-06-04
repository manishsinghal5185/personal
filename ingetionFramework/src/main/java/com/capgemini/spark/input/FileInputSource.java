package com.capgemini.spark.input;
import com.capgemini.commons.*;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.lit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.Path;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class FileInputSource implements InputSource<Row> {

    private static final Logger log= LoggerFactory.getLogger(FileInputSource.class);
    private List<dsFileRequest> dsFiles;
    @Override
    public void createDataset(ingestionRequest ir, SparkSession spark) throws Exception {
        dsFiles=ir.getDsFiles();
        for(dsFileRequest dsFile:dsFiles) {
            Dataset<Row> df=createDataSetForOneFile(dsFile,spark);
            String tmpTableName=dsFile.getDataFrameName();
            createDatasetTempTable(df,tmpTableName);
        }

    }
    private Dataset<Row> createDataSetForOneFile(dsFileRequest dsFile, SparkSession spark) throws Exception {
        Dataset<Row> df;
        switch (dsFile.getDsFileType().toUpperCase(Locale.ROOT)){
            case MetaConfigConst.CSV:
                log.info("Reading from source file");
                df=getDSFromTextFile(dsFile,spark);
                break;
            default:
                throw new Exception("input File type value can only be csv");
        }
        return df;
    }

    @Override
    public String createDatasetTempTable(Dataset<Row> df,String tmpTableName) throws Exception{
        log.info("creating dataFrame as:{}",tmpTableName);
        if (tmpTableName!=null)
            df.createOrReplaceTempView(tmpTableName);
        return tmpTableName;
    }

    private Dataset<Row> getDSFromTextFile(dsFileRequest dsFile, SparkSession spark) throws Exception {
        Dataset<Row> df;
        DataFrameReader dataFrameReader=spark.read();
        if (dsFile.getDsSchemaFileName()==null || dsFile.getDsSchemaFileName().isEmpty()){
            log.info("Setting spark options to inferschema");
            if (dsFile.isHeader()) {
                log.info("using header to infer column name");
                dataFrameReader.option("inferSchema", "true").option("header", "true");
            }else {
                throw new Exception("No Schema or Header is provided for file " + dsFile.getDsFileName());
            }

        }
        if (dsFile.getDsFileDelimiter()!=null) {
            log.info("delimiter is:{}", dsFile.getDsFileDelimiter());
            dataFrameReader.option("delimiter", dsFile.getDsFileDelimiter());
        }
        //spark.read().option("inferSchema", "true").option("header", "true").option("delimiter", dsFile.getDsFileDelimiter()).csv(dsFile.getDsFileName()).printSchema();
        Path srcPath=new Path(dsFile.getDsFileName());
        df=dataFrameReader.csv(dsFile.getDsFileName());
        //add pseudo coloumns to input file
        df=df.withColumn(MetaConfigConst.ds_filename,lit(srcPath.getName()));
        df=df.withColumn(MetaConfigConst.loading_dt,lit(CalendarUtil.calcCalendarCurrentDay()));
        return df;
    }
}
