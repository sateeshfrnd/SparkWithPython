'''
Created on May 5, 2017

@author: Satish Kumar

Usage:
Spark >1.4
    spark-submit --master yarn \
     --deploy-mode cluster  \
     --driver-memory 3G \
     --executor-memory 9G \     
     --packages com.databricks:spark-avro_2.10:2.0.1 \ 
     --packages com.databricks:spark-csv_2.10:1.5.0 \
     --packages com.databricks:spark-xml_2.10:0.4.1 \
     WriteReadDifferentFileFormats.py <INPUT_LOCATION_PATH> <OUTPUT_PATH>
     
Spark 2.0
    spark-submit --master yarn \
     --deploy-mode cluster  \
     --driver-memory 3G \
     --executor-memory 9G \     
     --packages com.databricks:spark-avro_2.11:3.1.0 \
     --packages com.databricks:spark-csv_2.11:1.5.0 \ 
     --packages com.databricks:spark-xml_2.11:0.4.1 \
     WriteReadDifferentFileFormats.py <INPUT_LOCATION_PATH> <OUTPUT_PATH>
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

def main(argv):
    inputPath = argv[0]
    targetRootPath = argv[1]
    # inputPath = 's3://my-workspace/spark/datasets/sampleData.txt'
    #targetRootPath = 's3://my-workspace/spark/outputs_may08/'
    
    ''' Set configuration for the SparkContext '''
    spark_conf = SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    
    ''' Create the SparkContext '''
    sc = SparkContext(conf=spark_conf)
    
    ''' Create context for handling DataFrame '''   
    spark, sqlContext = getContextForDataFrame(sc)
    
    ''' Remove all cached tables from the in-memory cache '''
    spark.catalog.clearCache()
    
    ''' Load sample TextFile '''
    data_rdd = sc.textFile(inputPath)
    
    ''' Filter records belongs to 'city1' and save as TextFile '''
    city1_rdd = data_rdd.map(lambda row : row.split(",")).filter(lambda x : x[3] == 'city1')
    city1_rdd.coalesce(1).saveAsTextFile(targetRootPath+'city1_textFile')
    
    ''' Create DataFrame from RDD'''
    mapped_rdd = data_rdd.map(lambda row : row.split(",")) \
                        .map(lambda x: Row(id=int(x[0]),name=x[1],email=x[2],city=x[3]))
                        
    data_df = mapped_rdd.toDF()
    data_df.printSchema()   # To check the schema
    data_df.show()          # To view the data in tabular form



    ''' Save DataFrame in AVRO and load
        Recommended way to read and write data in AVRO using Spark's Dataframe API's. 
        Supported compress types: uncompressed, snappy and deflate
        Reference: https://github.com/databricks/spark-avro    
    '''
    data_df.coalesce(1).write.format("com.databricks.spark.avro").mode("overwrite").save(targetRootPath+'data_avro')
    avro_df = spark.read.format("com.databricks.spark.avro").load(targetRootPath+'data_avro')

    # Apply compression while writing
    # configuration to use deflate compression
    
    version = sc.version
    if version[:1] == '2' :
        ''' For SPARK 2.0 '''
        spark.conf.set("spark.sql.avro.compression.codec", "deflate")
        spark.conf.set("spark.sql.avro.deflate.level", "5")
    else:
        sqlContext.setConf("spark.sql.avro.compression.codec", "deflate")
        sqlContext.setConf("spark.sql.avro.deflate.level", "5")

    avro_df.coalesce(1).write.format("com.databricks.spark.avro").save(targetRootPath+'data_avro_deflate')
    
    
    ''' Save DataFrame in PARQUET and load'''
    data_df.coalesce(1).write.format("parquet").mode("overwrite").save(targetRootPath+'data_parquet')   # Default snappy compression applies
    parquetData_df = spark.read.format("parquet").load(targetRootPath+'data_parquet')
    
    # Apply compression while writing
    data_df.coalesce(1).write.option("compression", "none").mode("overwrite").save(targetRootPath+'data_compress_none')
    data_df.coalesce(1).write.option("compression", "gzip").mode("overwrite").save(targetRootPath+'data_compress_gzip')
    data_df.coalesce(1).write.option("compression", "lzo").mode("overwrite").save(targetRootPath+'data_compress_lzo')
    data_df.coalesce(1).write.option("compression", "snappy").mode("overwrite").save(targetRootPath+'data_compress_snappy')
    data_df.coalesce(1).write.option("compression", "uncompressed").mode("overwrite").save(targetRootPath+'data_compress_uncompressed')
    
    spark.read.format("parquet").load(targetRootPath+'data_compress_none').show()
    spark.read.format("parquet").load(targetRootPath+'data_compress_gzip').show()
    spark.read.format("parquet").load(targetRootPath+'data_compress_lzo').show()
    spark.read.format("parquet").load(targetRootPath+'data_compress_snappy').show()
    spark.read.format("parquet").load(targetRootPath+'data_compress_uncompressed').show()
    
    ''' Save DataFrame in ORC and Load
        Supported compress codes : uncompressed, lzo, snappy, zlib, none
    '''
    data_df.coalesce(1).write.format("orc").mode("overwrite").save(targetRootPath+'data_orc')   # Default snappy compression applies
    orcData_df = spark.read.format("orc").load(targetRootPath+'data_orc')
    
    # Apply compression while writing
    data_df.coalesce(1).write.format("orc").option("compression", "zlib").mode("overwrite").save(targetRootPath+'data_orc_zlib')
    spark.read.format("orc").load(targetRootPath+'data_orc_zlib').show()
    
    
    ''' Save DataFrame in JSON and Load'''
    data_df.coalesce(1).write.mode("overwrite").json(targetRootPath+'data_json')
    jsonData_df = spark.read.json(targetRootPath+'data_json')
    
       
    ''' Save DataFrame in CSV and Load 
        Reference : https://github.com/databricks/spark-csv
    '''
    data_df.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save(targetRootPath+'data_csv')
    csv_df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load(targetRootPath+'data_csv')
    
    
    ''' Save DataFrame in XML and Load
        Reference: https://github.com/databricks/spark-xml
    '''
    data_df.coalesce(1).write.format("com.databricks.spark.xml").options(rowTag='employee', rootTag='employees').save(targetRootPath+'data_xml.xml')
    xml_df = sqlContext.read.format("com.databricks.spark.xml").option("rowTag", "employee").load(targetRootPath+'data_xml.xml')
        
    ''' stop spark context ''' 
    spark.sparkContext.stop()
    exit(0)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise Exception('Incorrect number of arguments passed')
    print 'Number of arguments:', len(sys.argv), 'arguments.'
    print 'Argument List:', str(sys.argv)
    main(sys.argv[1:])
