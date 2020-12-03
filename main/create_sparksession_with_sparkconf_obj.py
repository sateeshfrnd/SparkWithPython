import findspark

findspark.init()

from pyspark import SparkConf
from pyspark.sql import *

if __name__ == "__main__":
    conf = SparkConf()
    conf.set("spark.app.name", "PySpark")
    conf.set("spark.master", "local[3]")

    spark = (
        SparkSession.builder
            .config(conf=conf)
            .getOrCreate()
    )

    print("Spark Version = ",spark.version)
    print("Spark Configurations : ")
    get_conf = spark.sparkContext.getConf()
    print(get_conf.toDebugString())

    spark.stop()
