from pyspark import pipelines as dp
from pyspark.sql.functions import col

@dp.table
def silver_01():

    df = spark.readStream.table('bronze_02_cleansed')
    return df
