from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, regexp_extract

spark = SparkSession.builder.appName("GovTransform").master("local[*]").getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df_mps_raw = spark.read.format("json") \
    .option("recursiveFileLookup", "true") \
    .option("multiLine", "true") \
    .load("./data/raw/mps/") \
    .withColumn(
        "fileName",
        input_file_name()
    ) \
    .withColumn(
        "term",
        regexp_extract("fileName", r"term(\d+)", 1)
    )

df_votings_raw = spark.read.format("json") \
    .option("recursiveFileLookup", "true") \
    .option("multiLine", "true") \
    .load("./data/raw/votings/7/proceeding/") \
    .withColumn(
        "file_name", 
        input_file_name()
    ) \
    .withColumn(
        "term", 
        regexp_extract("file_name", r"term(\d+)", 1)
    ) \
    .withColumn(
        "proceeding",
        regexp_extract("file_name",r"proceeding(\d+)", 1)
    ) \
    .withColumn(
        "vote_no", 
        regexp_extract("file_name", r"vote(\d+)", 1)
    )
