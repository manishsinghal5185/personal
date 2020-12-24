from typing import Any


class DsQuery:
    def __init__(self):
        pass

    def __init__(self, dsQueryName, resultTempTableName,rawSql):
        self.__dsQueryName=dsQueryName
        self.__resultTempTableName=resultTempTableName
        self.__rawSql=rawSql


    @property
    def dsQueryName(self):
        return self.__dsQueryName
    
    
    @dsQueryName.setter
    def dsQueryName(self, dsQueryName):
        self.__dsQueryName = dsQueryName

    @property
    def resultTempTableName(self):
        return self.__resultTempTableName

    @resultTempTableName.setter
    def resultTempTableName(self, resultTempTableName):
        self.__resultTempTableName = resultTempTableName

    @property
    def rawSql(self):
        return self.__rawSql

    @rawSql.setter
    def rawSql(self, rawSql):
        self.__rawSql = rawSql

class DataTarget:
    def __init__(self):
        pass

    def __init__(self, targetName, resutFormat,targetTableName,headers,overwriteFlag):
        self.__targetName=targetName
        self.__resutFormat=resutFormat
        self.__targetTableName=targetTableName
        self.__headers = headers
        self.__overwriteFlag = overwriteFlag

    @property
    def targetName(self):
        return self.__targetName

    @targetName.setter
    def targetName(self, targetName):
        self.__targetName = targetName

    @property
    def resutFormat(self):
        return self.__resutFormat

    @resutFormat.setter
    def resutFormat(self, resutFormat):
        self.__resutFormat = resutFormat

    @property
    def targetTableName(self):
        return self.__targetTableName

    @targetTableName.setter
    def targetTableName(self, targetTableName):
        self.__targetTableName = targetTableName

    @property
    def headers(self):
        return self.__headers

    @headers.setter
    def headers(self, headers):
        self.__headers = headers

    @property
    def overwriteFlag(self):
        return self.__overwriteFlag

    @overwriteFlag.setter
    def overwriteFlag(self, overwriteFlag):
        self.__overwriteFlag = overwriteFlag