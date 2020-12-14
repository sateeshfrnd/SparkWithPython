import sys

import findspark

findspark.init()
from utils.logger import Log4J
from pyspark.sql import SparkSession


def main(
        spark: SparkSession,
        inputfile: str,
):
    logger.info(inputfile)

    flights_df = (
        spark.read
            .format("parquet")
            .load(inputfile)
    )

    spark.sql("CREATE DATABASE IF NOT EXISTS AIRLINE_DB")
    spark.catalog.setCurrentDatabase("AIRLINE_DB")

    # Write DataFrame table with Partitions
    flights_df.write \
        .mode('overwrite') \
        .partitionBy("OP_CARRIER", "ORIGIN") \
        .saveAsTable("flight_data")

    flights_df.write \
        .mode('overwrite') \
        .bucketBy(5, "OP_CARRIER", "ORIGIN") \
        .sortBy("OP_CARRIER", "ORIGIN") \
        .saveAsTable("flight_data_bucket")

    logger.info(spark.catalog.listTables("AIRLINE_DB"))
    logger.info("done")


if __name__ == "__main__":
    spark = (
        SparkSession.builder
            .appName("PySpark")
            .master("local[3]")
            .getOrCreate()
    )

    logger = Log4J(spark)

    if len(sys.argv) != 2:
        logger.error("Usage: ControlDataSink <input_file>")
        sys.exit(-1)

    input_file = sys.argv[1]

    main(spark, input_file)

    spark.stop()
