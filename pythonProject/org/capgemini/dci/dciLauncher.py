from pyspark import SparkContext
from pyspark.sql import SparkSession
import json
from pprint import pprint
import sys, traceback

# define top level module logging
spark = SparkSession.builder.appName('test').getOrCreate()
#logging= spark.sparkContext._jvm.org.apache.log4j.LogManager.getLogger(__name__)
Logger= spark._jvm.org.apache.log4j.Logger
logging = Logger.getLogger(__name__)
print(type(logging))
# logging = logging.getlogging(__name__)
if True:
    df=spark.read.csv('C:/Users/msingha1/workspace/DCI/sample.csv',header=True)
partKey=['Industry_aggregation_NZSIOC','Industry_code_NZSIOC','asdad']
df.printSchema()

#df.write.mode('overwrite').partitionBy(partKey).csv('C:/Users/msingha1/workspace/DCI/target/repart.csv',header=True)
#df.write.mode('overwrite').partitionBy(partKey).option('path','C:/Users/msingha1/workspace/DCI/target/repart.csv',header=True).saveAsTable()
# class dciLauncher:
#     def parseConfig(file):
#         #file="C:/Users/msingha1/workspace/DCI/templateConfig.json"
#         with open(file) as f:
#             data = json.load(f)
#         return data
#
#     def validateParameters(data):
#         try:
#             #pprint(data)
#             dsQueryNode=data['dataSource']['dsQueries']
#
#             stagingLocation=data['staging']['stagingLocation']
#             targetTableName=data['dataTarget']['targetTableName']
#             resutFormat=data['dataTarget']['resutFormat']
#             for dsQuery in dsQueryNode:
#                 resultTempTableName = dsQuery['dsQuery']['resultTempTableName']
#                 rawSql = dsQuery['dsQuery']['rawSql']
#                 if(resultTempTableName):
#                     logging.info("resultTempTableName is="+resultTempTableName)
#                 else:
#                     logging.error("resultTempTableName not found, exiting the process")
#                     exit(1)
#                 if(rawSql):
#                     logging.info("raqSQL is="+rawSql)
#                 else:
#                     logging.error("rawSQL not found")
#                     exit(1)
#             if (stagingLocation):
#                 logging.info("stagingLocation is=" + rawSql)
#             else:
#                 logging.error("stagingLocation not found")
#                 exit(1)
#             if (targetTableName):
#                 logging.info("targetTableName is=" + rawSql)
#             else:
#                 logging.error("targetTableName not found")
#                 exit(1)
#             if (resutFormat):
#                 logging.info("resutFormat is=" + rawSql)
#             else:
#                 logging.error("resutFormat not found")
#                 exit(1)
#         except Exception as e:
#             logging.error(f'exception in component, failed with the following error')
#             traceback.print_exc(file=sys.stdout)
#             exit(1)
