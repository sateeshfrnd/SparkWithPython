import findspark

findspark.init()

from pyspark.sql import *

if __name__ == "__main__":
    spark = (
        SparkSession.builder
            .appName("PySpark")
            .master("local")
            .getOrCreate()
    )

    print(spark.version)
    get_conf = spark.sparkContext.getConf()
    print(get_conf.toDebugString())

    spark.stop()
