import sys

import findspark

findspark.init()

from pyspark import SparkConf
from pyspark.sql import *
from collections import namedtuple


def main(fileName):
    print(f"Filename = {fileName}")
    conf = SparkConf()
    conf.set("spark.app.name", "PySpark")
    conf.set("spark.master", "local[3]")

    spark = (
        SparkSession.builder
            .config(conf=conf)
            .getOrCreate()
    )

    sc = spark.sparkContext

    linesRDD = sc.textFile(fileName)
    partitionedRDD = linesRDD.repartition(2)
    colsRDD = partitionedRDD.map(lambda line: line.replace('"', '').split(","))

    SurveyRecord = namedtuple("SurveyRecord", ["Age", "Gender", "Country", "State"])
    selectRDD = colsRDD.map(lambda cols: SurveyRecord(int(cols[1]), cols[2], cols[3], cols[4]))

    filteredRDD = selectRDD.filter(lambda r: r.Age < 40)
    kvRDD = filteredRDD.map(lambda r: (r.Country, 1))
    countRDD = kvRDD.reduceByKey(lambda v1, v2: v1 + v2)

    colsList = countRDD.collect()

    for x in colsList:
        print(x)

    spark.stop()


if __name__ == "__main__":

    if len(sys.argv) != 2:
        print("Error: Pass argument as <filename>")
        sys.exit(-1)
    else:
        main(sys.argv[1])
