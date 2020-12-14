import findspark

findspark.init()
from utils.logger import Log4J
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *


def convert_string_to_date(
        df: DataFrame,
        formatter: str,
        column_name: str,
):
    return df.withColumn(column_name, to_date(col(column_name), formatter))


def main(spark: SparkSession):
    logger.info(spark.version)
    data_schema = StructType([
        StructField("id", IntegerType()),
        StructField("event_date", StringType())])

    data = [Row(1, "04/02/2020"), Row(2, "05/15/2020"), Row(3, "10/07/2020"), Row(4, "06/15/2020")]
    rdd = spark.sparkContext.parallelize(data, 2)
    df = spark.createDataFrame(rdd, data_schema)

    df.printSchema()
    df.show()

    df1 = convert_string_to_date(df, "MM/dd/yyyy", "event_date")
    df1.printSchema()
    df1.show()


if __name__ == "__main__":
    spark = (
        SparkSession.builder
            .appName("PySpark")
            .master("local")
            .getOrCreate()
    )

    logger = Log4J(spark)
    main(spark)

    spark.stop()
