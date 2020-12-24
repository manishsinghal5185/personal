import json
import sys
import traceback
from pyspark.sql.session import SparkSession

import logging

class dciLauncher:
    def parseConfig(file):
        #file="C:/Users/msingha1/workspace/DCI/templateConfig.json"
        with open(file) as f:
            data = json.load(f)
        return data


class DsQueryRequest:
    def __init__(self,data):
        #self.validateParameters(data)
        self.appName=data['appName']
        self.sorName=data['sorName']
        self.appId=data['appId']
        self.subjectArea=data['subjectArea']
        self.dsQueries=[]
        for dsQuery in data['dataSource']['dsQueries']:
            self.dsQueries.append(DsQuery(dsQuery))
        self.staging=Staging(data['staging'])
        self.dataTarget=DataTarget(data['dataTarget'])
        self.sparkConfigParams=data['sparkConfigParams']

class DsQuery:
    def __init__(self, data):
        self.dsQueryName = data['dsQuery']['dsQueryName']
        self.resultTempTableName = data['dsQuery']['resultTempTableName']
        self.rawSql = data['dsQuery']['rawSql']

class DataTarget:
    def __init__(self, data):
        self.targetName=data['targetName']
        self.resutFormat=data['resutFormat']
        self.targetTableName=data['targetTableName']
        self.headers=data['headers']
        self.overwriteFlag=data['overwriteFlag']

class Staging:
    def __init__(self, data):
        self.stagingLocation=data['stagingLocation']
        self.partiton=data['partiton']

class sparkSQLLauncher:
    def __init__(self):
      pass
    def sparkLauncher(self,dsQueryRequest):
        spark = SparkSession \
            .builder \
            .appName(dsQueryRequest.appName) \
            .enableHiveSupport() \
            .config('spark.sql.hive.convertMetastoreParquet', False) \
            .config("mapred.input.dir.recursive", "true")\
            .config("mapreduce.input.fileinputformat.input.dir.recursive", "true")\
            .config("spark.sql.hive.convertMetastoreParquet", "false")\
            .getOrCreate()


        Logger = spark._jvm.org.apache.log4j.Logger
        logger = Logger.getLogger(__name__)
        if dsQueryRequest.sparkConfigParams:
            for k, v in dsQueryRequest.sparkConfigParams.items():
                print(f"spark configuration key={k.strip()} and value is {v}")
                spark.conf.set(k.strip(), v)

        # run queries

        for dsquery in dsQueryRequest.dsQueries:
            df = self.runOneDsQuery(dsquery, spark, logger)
            df.createOrReplaceTempView(dsquery.resultTempTableName)
            print(df.count())
        print('storing final result into the hdfs..')

        self.storeTargetResult(df,dsQueryRequest)
        stagingLoc = dsQueryRequest.staging.stagingLocation.rstrip(
            '/') + '/' + dsQueryRequest.dataTarget.targetTableName
        targetTableName = dsQueryRequest.dataTarget.targetTableName
        print('Creating the final target table '+dsQueryRequest.dataTarget.targetTableName)
        self.createTargetTable(spark,dsQueryRequest)

    def createTargetTable(self, spark, dsQueryRequest):
        if (dsQueryRequest.staging.stagingLocation):
            stagingLoc = dsQueryRequest.staging.stagingLocation.rstrip('/') + '/' + dsQueryRequest.dataTarget.targetTableName+'/*'
        targetTableName=dsQueryRequest.dataTarget.targetTableName
        format = dsQueryRequest.dataTarget.resutFormat.lower()
        dropSQL = 'DROP TABLE IF EXISTS '+targetTableName
        print(dropSQL)
        spark.sql(dropSQL)
        if format=='csv':
            sql='CREATE TABLE if not exists '+targetTableName+' USING '+format+' OPTIONS(path "'+stagingLoc+'", header "true")'
            print(sql)
            spark.sql(sql)
        else:
            sql='CREATE TABLE if not exists ' + targetTableName + ' USING ' + format + ' OPTIONS(path "' + stagingLoc + '")'
            print(sql)
            spark.sql(sql)

    # def createTargetTable(self,spark,dsQueryRequest,df):
    #     if (dsQueryRequest.staging.stagingLocation):
    #         stagingLoc = dsQueryRequest.staging.stagingLocation.rstrip(
    #             '/') + '/' + dsQueryRequest.dataTarget.targetTableName+'/*'
    #     targetTableName=dsQueryRequest.dataTarget.targetTableName
    #     format = dsQueryRequest.dataTarget.resutFormat.lower()
    #     if format=='csv':
    #         format='TEXTFILE'
    #     partKeys = []
    #     if (dsQueryRequest.staging.partiton):
    #         partitionKeys = dsQueryRequest.staging.partiton
    #         partKeys = self.validatePartitonKeys(df, partitionKeys)
    #     schema = df.schema
    #     cols = []
    #     partCols = []
    #     for field in schema:
    #         if partKeys.__contains__(field.name.lower()):
    #             partCols.append(field.name + " " + field.dataType.simpleString())
    #         else:
    #             cols.append(field.name + " " + field.dataType.simpleString())
    #     columns=','.join(cols).lower()
    #     partitonColumns=','.join(partCols).lower()
    #     createTableSqlWithPartiton='create external table if not exists '+targetTableName+' ('+columns+' ) partitioned by ('+partitonColumns.lower()+' ) stored as '+format+' location \'' + stagingLoc+'\''
    #     createTableSql = 'create external table if not exists ' + targetTableName + ' (' + columns + ' ) stored as ' + format + ' location \'' + stagingLoc+'\''
    #     if partitonColumns:
    #         print('createTableSqlWithPartiton='+createTableSqlWithPartiton)
    #         spark.sql(createTableSqlWithPartiton)
    #     else:
    #         print('createTableSql='+createTableSql)
    #         spark.sql(createTableSql)

    def storeTargetResult(self,df,dsQueryRequest):
        print('print df inside storeTargetResult :', df.count())
        if(dsQueryRequest.dataTarget.overwriteFlag):
            writeMode='overwrite'
        else:
            writeMode = 'append'
        print('writeMode is '+writeMode)
        if(dsQueryRequest.staging.stagingLocation):
            stagingLoc=dsQueryRequest.staging.stagingLocation.rstrip('/')+'/'+dsQueryRequest.dataTarget.targetTableName
            print('stagingLocation is '+stagingLoc)
        partKeys=[]
        if(dsQueryRequest.staging.partiton):
            partitionKeys=dsQueryRequest.staging.partiton
            partKeys=self.validatePartitonKeys(df,partitionKeys)
            print('partitonKeys are '+','.join(partKeys))
        #writing data into the target
        format=dsQueryRequest.dataTarget.resutFormat.lower()
        listOfFomat=['csv','parquet']
        if format in [x.lower() for x in listOfFomat]:
            if format=='csv':
                if partKeys:
                    print('writing data in HDFS with '+format+' format and with partition')
                    df.write.mode(writeMode).partitionBy(partKeys).format(format).save(stagingLoc, header=True)
                else:
                    print('writing data in HDFS with ' + format)
                    df.write.mode(writeMode).format(format).save(stagingLoc, header=True)
            elif format=='parquet':
                if partKeys:
                    print('writing data in HDFS with ' + format + ' format and with partition')
                    df.write.mode(writeMode).partitionBy(partKeys).parquet(stagingLoc)
                else:
                    print('writing data in HDFS with ' + format)
                    df.write.mode(writeMode).parquet(stagingLoc)


    def validatePartitonKeys(self,df,partitionKeys):
        schema=df.schema
        partKeys=[]
        for k in partitionKeys:
            k=k.lower()
            if schema.fieldNames().__contains__(k):
                partKeys.append(k)
            else:
                print(k+' is not a valid partition key, ignoing '+k)
        return partKeys

    def runOneDsQuery(self,dsQuery, spark, logger):
        sql = dsQuery.rawSql
        logger.info("raw SQL="+sql)
        if not sql:
            logger.error("raw SQL is empty, exiting the process")
            traceback.print_exc(file=sys.stdout)
            exit(1)

        df = spark.sql(sql)

        return df


data = dciLauncher.parseConfig("/tmp/dimond-2.json")
dsQueryRequest = DsQueryRequest(data)
SSl=sparkSQLLauncher()
SSl.sparkLauncher(dsQueryRequest)