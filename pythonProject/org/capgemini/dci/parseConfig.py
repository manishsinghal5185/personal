
format='csv'
listOfFomat=['cSv','parquet']
print(listOfFomat.__str__()+"asdasd")
if format in [x.lower() for x in listOfFomat]:
    print(True)
else:
    print(False)