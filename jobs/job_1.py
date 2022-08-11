"""
Job 1 creates a simple Dataframe and saves it to dbfs.
"""

from utils import start_spark_session_and_dbutils


def job_1():
    spark, dbutils = start_spark_session_and_dbutils()

    df = spark.createDataFrame(
        [
            (2019, 1.0),
            (2019, 3.0),
            (2019, 5.0),
            (2019, 2.0),
            (2019, 4.0),
            (2019, 6.0),
            (2021, 2.0),
            (2021, 4.0),
            (2021, 8.0),
            (2021, 9.0),
            (2021, 11.0),
            (2021, 13.0),
        ],
        ["year", "price"]
    )
    df.write("dbfs:/myfiles/mydataframe.parquet")
