"""
Job 2 reads dataframe from dbfs finds mean and saves results to dbfs.
"""

from utils import start_spark_session_and_dbutils
import pyspark.sql.functions as F


def read_df(spark):
    return spark.read.parquet('dbfs:/myfiles/mydataframe.parquet')


def job_2():
    spark, dbutils = start_spark_session_and_dbutils()

    df = read_df(spark)

    df = df.groupby("year").agg(F.mean("price").alias("yearly_mean_price"))

    df.write("dbfs:/myfiles/mydataframe_transformed.parquet")
