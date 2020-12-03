import findspark

findspark.init()

from pyspark.sql import *

from utils.sparkutils import get_spark_app_config

if __name__ == "__main__":
    conf = get_spark_app_config()
    print(conf.toDebugString())

    spark = (
        SparkSession.builder
            .config(conf=conf)
            .getOrCreate()
    )

    print(f"Spark Version = {spark.version}")
    print("Spark Configurations : ")
    get_conf = spark.sparkContext.getConf()
    print(get_conf.toDebugString())

    spark.stop()
