
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