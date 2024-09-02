from IPython.core.display_functions import display
from pyspark.sql import *

spark = SparkSession.builder \
    .appName("sf-fire-calls") \
    .master('local[3]') \
    .getOrCreate()

fire_df = spark.read \
    .format("csv") \
    .option("header", True) \
    .option('inferSchema', True) \
    .load('/home/luffy/Documents/Data_Science/Spark/raw_data/sf_fire_calls_data/fire-incidents.csv')

fire_df.show()