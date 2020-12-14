import sys
import findspark

findspark.init()
from utils.logger import Log4J
from pyspark.sql import *
from pyspark.sql.types import *

def main(spark : SparkSession, fileName: str):
    logger.info(spark.version)
    logger.info(fileName)
    survey_df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema","true")
        .option("mode", "FAILFAST")
        .load(fileName)
    )
    survey_df.printSchema()
    survey_df.show(5)

    # Define Schema programmatically
    survey_schema_struct = StructType([
        StructField("Timestamp", DateType()),
        StructField("Age", IntegerType()),
        StructField("Gender", StringType()),
        StructField("Country", StringType()),
        StructField("state", StringType()),
        StructField("self_employed", StringType()),
        StructField("family_history", StringType()),
        StructField("treatment", StringType()),
        StructField("work_interfere", StringType()),
        StructField("no_employees", StringType()),
        StructField("remote_work", StringType()),
        StructField("tech_company", StringType()),
        StructField("benefits", StringType()),
        StructField("care_options", StringType()),
        StructField("wellness_program", StringType()),
        StructField("seek_help", StringType()),
        StructField("anonymity", StringType()),
        StructField("leave", StringType()),
        StructField("mental_health_consequence", StringType()),
        StructField("phys_health_consequence", StringType()),
        StructField("coworkers", StringType()),
        StructField("supervisor", StringType()),
        StructField("mental_health_interview", StringType()),
        StructField("phys_health_interview", StringType()),
        StructField("mental_vs_physical", StringType()),
        StructField("obs_consequence", StringType()),
        StructField("comments", StringType()),
    ])

    survey_schema_df = (
        spark.read
            .format("csv")
            .option("header", "true")
            .schema(survey_schema_struct)
            .option("mode", "FAILFAST")
            # .option("dateFormat", "YYYY-MM-dd hh:mm:ss")
            .load(fileName)
    )
    survey_schema_df.printSchema()
    survey_schema_df.show(5)

    logger.info("Schema:" + survey_schema_df.schema.simpleString())

    # Define Schema DDL String
    survey_schema_ddl = """
        Timestamp date,Age int,Gender string,Country string,state string,self_employed string,family_history string,treatment string,work_interfere string,no_employees string,remote_work string,tech_company string,benefits string,care_options string,wellness_program string,seek_help string,anonymity string,leave string,mental_health_consequence string,phys_health_consequence string,coworkers string,supervisor string,mental_health_interview string,phys_health_interview string,mental_vs_physical string,obs_consequence string,comments string
    """
    survey_schema_ddl_df = (
        spark.read
            .format("csv")
            .option("header", "true")
            .schema(survey_schema_ddl)
            .option("mode", "FAILFAST")
            # .option("dateFormat", "YYYY-MM-dd hh:mm:ss")
            .load(fileName)
    )
    survey_schema_ddl_df.printSchema()
    survey_schema_ddl_df.show(5)


if __name__ == "__main__":
    spark = (
        SparkSession.builder
            .appName("PySpark")
            .master("local")
            .getOrCreate()
    )

    logger = Log4J(spark)

    if len(sys.argv) != 2:
        logger.error("Usage: HelloSpark <filename>")
        sys.exit(-1)

    fileName = sys.argv[1]

    main(spark,fileName)

    spark.stop()