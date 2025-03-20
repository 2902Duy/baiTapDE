
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType


sp = SparkSession.builder \
    .appName("DE-ETL-102")\
    .master("local[*]")\
    .config("spark.executor.memory","4g")\
    .getOrCreate()

data = [("11/12/2025",),("27/02.2014",),("2023.01.09",),("28-12-2005",)]


def slip_date(date):
    if(date[2].isdigit()):
        return int(date[8:]), int(date[5:7]),int(date[:4])
    else:
        return int(date[:2]),int(date[3:5]),int(date[6:])
    ## return day month year

schema = StructType([
    StructField("day", IntegerType(), True),
    StructField("month", IntegerType(), True),
    StructField("year", IntegerType(), True)
])
df = sp.createDataFrame(data, ["date"])
slip_date_udf = udf(slip_date, schema)
df_parsed = df.withColumn("dmy", slip_date_udf("date")) \
              .select("date", "dmy.*")

df_parsed.show(truncate=False)
