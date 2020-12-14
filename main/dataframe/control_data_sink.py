import sys

import findspark

findspark.init()
from utils.logger import Log4J
from pyspark.sql import *
from pyspark.sql.functions import spark_partition_id


def main(
        spark: SparkSession,
        inputfile: str,
        output_dir: str
):
    logger.info(spark.version)
    logger.info(inputfile)
    logger.info(output_dir)
    flights_df = (
        spark.read
            .format("parquet")
            .load(inputfile)
    )

    # Write DataFrame in parquet format
    logger.info("flights_df : Number of Partitions : " + str(flights_df.rdd.getNumPartitions()))
    flights_df.groupBy(spark_partition_id()).count().show()
    # flights_df.write.mode('overwrite').parquet(f"{output_dir}org/")

    flights_partition_df = flights_df.repartition(5)
    logger.info("flights_partition_df : Number of Partitions : " + str(flights_partition_df.rdd.getNumPartitions()))
    flights_partition_df.groupBy(spark_partition_id()).count().show()

    # Write DataFrame with Partitions
    flights_partition_df.write \
        .mode('overwrite') \
        .partitionBy("OP_CARRIER", "ORIGIN") \
        .parquet(f"{output_dir}part_data/")

    # Write DataFrame with Partitions and control file sizes
    flights_partition_df.write \
        .mode('overwrite') \
        .partitionBy("OP_CARRIER", "ORIGIN") \
        .option("maxRecordsPerFile", 10000) \
        .parquet(f"{output_dir}part_size_data/")

    logger.info("done")


if __name__ == "__main__":
    spark = (
        SparkSession.builder
            .appName("PySpark")
            .master("local[3]")
            .getOrCreate()
    )

    logger = Log4J(spark)

    if len(sys.argv) != 3:
        logger.error("Usage: ControlDataSink <input_file> <output_dir>")
        sys.exit(-1)

    input_file = sys.argv[1]
    output_dir = sys.argv[2]

    main(spark, input_file, output_dir)

    spark.stop()
