
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType
from datetime import datetime

sp = SparkSession.builder \
    .appName("DE-ETL-102")\
    .master("local[*]")\
    .config("spark.executor.memory","4g")\
    .getOrCreate()

data = [("11/12/2025",),("27/02.2014",),("2023.01.09",),("28-12-2005",)]

df = sp.createDataFrame(data,["date_raw"])

@udf(StringType())
def format_date(date_str):
    date_formats = [
        "%d-%m-%Y",
        "%Y-%m-%d",
        "%d-%m-%y",
    ]
    clean_str = date_str.replace('.', '-').replace('/', '-')
    for fmt in date_formats:
        try:
            parsed_date = datetime.strptime(clean_str, fmt)
            return parsed_date.strftime("%Y-%m-%d")
        except ValueError:
            continue

df_result = (df.withColumn(
    "date_str", format_date("date_raw")
).withColumn(
    "date_format", to_date("date_str")
).withColumn(
    "day",day("date_format")
).withColumn(
    "month",month("date_format")
).withColumn(
    "year",year("date_format")
)
.select("date_raw", "date_format","day","month","year"))

df_result.show(truncate=False)
