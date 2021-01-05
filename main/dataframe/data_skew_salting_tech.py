import findspark

findspark.init()
from utils.logger import Log4J
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as f


# Apply Salting technique to handle data skewness
def remove_dataskew(
        left_table: DataFrame,
        left_col_name: str,
        right_table: DataFrame
):
    _random_val = 3

    df1 = left_table.withColumn(
        "exploded_col",
        f.concat(
            left_table[left_col_name],
            f.lit("-"),
            f.lit(f.floor(f.rand(123456) * _random_val))
        )
    )

    _array = f.array(
        [
            f.lit(i) for i in range(1, _random_val)
        ]
    )

    df2 = right_table.select(
        *right_table.columns,
        f.explode(
            _array
        ).alias("explode_col"),
    ).withColumn(
        "exploded_col",
        f.concat_ws("-", f.col(left_col_name), f.col("explode_col"))
    ).drop("explode_col")

    return df1, df2


def main(spark: SparkSession):
    logger.info(spark.version)

    d1 = [
        ('x', 3),
        ('x', 2),
        ('x', 1),
        ('x', 4),
        ('y', 3),
        ('z', 3),
    ]

    df1 = spark.createDataFrame(d1, ['id', 'value1'])

    d2 = [
        ('x', 13),
        ('x', 12),
        ('y', 3),
        ('z', 3),
    ]

    df2 = spark.createDataFrame(d2, ['id', 'value2'])

    d12 = df1.join(df2, df1.id == df2.id).drop(df2.id)
    # d12.show()
    # d12.select("id").groupBy("id").agg(f.count("*")).show()

    df3, df4 = remove_dataskew(df1, "id", df2)
    d34 = df3.join(df4, "exploded_col").drop(df4.id).drop("exploded_col")
    d34.show()

    d34.select("id").groupBy("id").agg(f.count("*")).show()


if __name__ == "__main__":
    spark = (
        SparkSession.builder
            .appName("PySpark")
            .master("local[3]")
            .getOrCreate()
    )

    logger = Log4J(spark)

    main(spark)

    spark.stop()
