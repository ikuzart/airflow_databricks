from pyspark.sql import SparkSession


def start_spark_session_and_dbutils():
    """Getting SparkSession and dbutils. If run on Datbricks they're already available"""
    try:
      spark_ = spark
    except NameError:
        spark_ = (
            SparkSession.builder
                .master("local[*]")
                .config("spark.driver.bindAddress", "localhost")
                .appName("airflow_databricks")
                .getOrCreate()
        )
    dbutils = None
    if spark_.conf.get("spark.databricks.service.client.enabled") == "true":
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark_)
    else:
        import IPython
        dbutils = IPython.get_ipython().user_ns["dbutils"]
    return spark_, dbutils
