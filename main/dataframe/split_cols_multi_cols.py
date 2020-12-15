import findspark

findspark.init()
from utils.logger import Log4J
from pyspark.sql import SparkSession
import pyspark.sql.functions as f

def main(spark: SparkSession):
    data = [
        ('user1', 'Amaravati,AP,India'),
        ('user2', 'Karnataka,India'),
        ('user3', 'Hyderabad,Telangana,India'),
        ('user4', 'India'),
    ]

    df = spark.createDataFrame(data, ['cust_name', 'full_address'])
    df.show()

    df1 = (
        df.select(
            f.col("cust_name"),
            f.reverse(f.split(df.full_address,",")).alias("address")
        )
    )

    df1.printSchema()
    df1.show(truncate = False)

    result_df = df1.select (
        f.col("cust_name"),
        (f.col("address")[2]).alias("city"),
        (f.col("address")[1]).alias("state"),
        (f.col("address")[0]).alias("country"),
    )

    result_df.printSchema()
    result_df.show(truncate=False)


if __name__ == "__main__":
    spark = (
        SparkSession.builder
            .appName("Split Columns")
            .master("local[3]")
            .getOrCreate()
    )

    logger = Log4J(spark)

    main(spark)

    spark.stop()