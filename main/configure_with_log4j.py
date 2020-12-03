import findspark

from utils.logger import Log4J

findspark.init()

from pyspark.sql import *


if __name__ == "__main__":
    spark = (
        SparkSession.builder
            .appName("PySpark")
            .master("local")
            .getOrCreate()
    )

    logger = Log4J(spark)
    logger.info(spark.version)
    get_conf = spark.sparkContext.getConf()
    logger.info(get_conf.toDebugString())

    spark.stop()
