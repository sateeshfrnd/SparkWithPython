'''
Created on May 8, 2017

@author: suppar
'''

import sys
from sys import argv

try:
    from pyspark import SparkContext
    from pyspark import SparkConf
    from pyspark.sql.types import Row
except ImportError as e:
    print ("Error importing Spark Modules", e)
    sys.exit(1)
  
def getContextForDataFrame(sc):
    version = sc.version
    if version[:1] == '2' :
        try:
            from pyspark.sql import SparkSession
        except ImportError as e:
            print ("Error importing SparkSession Modules", e)
            sys.exit(1)
        spark = SparkSession.builder.enableHiveSupport().getOrCreate()
        spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        sqlContext =spark
    else :
        try:
            from pyspark.sql import HiveContext
            from pyspark.sql import SQLContext
        except ImportError as e:
            print ("Error importing HiveContext Modules", e)
            sys.exit(1)
        spark = HiveContext(sc)
        sqlContext =SQLContext(sc)
    return spark, sqlContext
  
''' Set configuration for the SparkContext '''
spark_conf = SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    
''' Create the SparkContext '''
sc = SparkContext(conf=spark_conf)
    
''' Create context for handling DataFrame '''  
hiveContext, sqlContext = getContextForDataFrame(sc)

''' =============== CREATE DATAFRAME ======================='''
''' Create DataFrame from List '''
l = [('Teja', 3)]
sqlContext.createDataFrame(l).collect()    #    [Row(_1=u'Teja', _2=3)]
sqlContext.createDataFrame(l, ['Name', 'Age']).collect()    #    [Row(Name=u'Teja', Age=1)]

''' Create DataFrame from Dict '''
d = [{'Name':'Teja', 'Age':1}]
sqlContext.createDataFrame(d).collect() #    [Row(Age=1, Name=u'Teja')]

''' Create DataFrame from RDD '''
rdd = sc.parallelize([('Teja', 3)])
sqlContext.createDataFrame(rdd).collect()   #    [Row(_1=u'Teja', _2=3)]
sqlContext.createDataFrame(rdd, ['Name', 'Age']).collect()   # [Row(Name=u'Teja', Age=3)]

from pyspark.sql import Row
Person = Row('Name','Age')
person = rdd.map(lambda row: Person(*row))
person_df = sqlContext.createDataFrame(person)
person_df.collect()     #    [Row(Name=u'Teja', Age=3)]

from pyspark.sql.types import *
structFields = [StructField("Name", StringType(),True),
                StructField("Age", IntegerType(),True) ]
schema = StructType(structFields)
person_df1 = sqlContext.createDataFrame(rdd, schema)
person_df1.collect()    #    [Row(Name=u'Teja', Age=3)]


''' ============== Register and Handle temp table to a DataFrame ============================'''
sqlContext.registerDataFrameAsTable(person_df, "person")
person_df2 = sqlContext.sql("SELECT * FROM person")
person_df2.collect()    #    [Row(Name=u'Teja', Age=3)]

''' Get the list of registed tables ''' 
getTables = sqlContext.tableNames()
getTablesAsDataFrame = sqlContext.tables()

"person" in sqlContext.tableNames() # True
sqlContext.dropTempTable("person")
"person" in sqlContext.tableNames() # False





