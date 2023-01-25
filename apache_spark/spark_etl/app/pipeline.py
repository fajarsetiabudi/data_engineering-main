from pyspark.sql import SparkSession

if  __name__ == '__main__':
    spark = SparkSession.\
        builder.\
        appName('reading_csv').\
        config('spark.driver.extraClassPath', '/jar/mysql-connector-java-8.0.29.jar').\
        getOrCreate()
 
# Extract Data       
filePath = 'home/fajarsetia/Documents/spark/app/data/covid-19-all.csv'
data = spark.read.csv(filePath, header=True, sep=",")

data.registerTempTable('covid_china') #simpan data dalam temptable

# Transform Data
output = spark.sql('SELECT * from covid_china WHERE Country = "China" AND Confirmed IS NOT NULL')
output.show()

output.write.format('json').save('filtered.json') # simpan data dalam format .json

#Load data
output.write.format("jdbc")\
    .option("url", "jdbc:mysql://localhost/covid")\
    .option("dbtable", "info")\
    .option("user", "root")\
    .option("password", "")\
    .mode("append")\
    .save()

