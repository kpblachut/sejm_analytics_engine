from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, regexp_extract, col

spark = SparkSession.builder.appName("GovTransform").master("local[*]").getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df_mps_raw = spark.read \
    .format("json") \
    .option("recursiveFileLookup", "true") \
    .option("multiLine", "true") \
    .load("../data/raw/mps/") \
    .withColumn(
        "fileName",
        input_file_name()
    ) \
    .withColumn(
        "term",
        regexp_extract("fileName", r"term(\d+)", 1)
    )

df_mps_clean = df_mps_raw \
    .select(
        col("id").cast("int").alias("mp_id"),
        col("firstName").alias("first_name"),
        col("lastName").alias("last_name"),
        col("birthDate").alias("birth_date"),
        col("birthLocation").alias("birth_location"),
        col("club"),
        col("active"),
        col("districtName").alias("district_name"),
        col("districtNum").alias("district_num"),
        col("educationLevel").alias("education_level"),
        col("numberOfVotes").alias("number_of_votes"),
        col("voivodeship"),
        col("profession"),
        col("term").cast("int")
    )

df_votings_raw = spark.read \
    .format("json") \
    .option("recursiveFileLookup", "true") \
    .option("multiLine", "true") \
    .load("../data/raw/votings/") \
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
        "vote_id", 
        regexp_extract("file_name", r"vote(\d+)", 1)
    )

df_votings_clean = df_votings_raw \
    .select(
        col("MP").cast("int").alias("mp_id"),
        col("vote"),
        col("term").cast("int"),
        col("proceeding").cast("int"),
        col("vote_id").cast("int")
    )

df_mps_clean.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .partitionBy("term") \
    .parquet("../data/silver/mps")

df_votings_clean.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .partitionBy("term") \
    .parquet("../data/silver/votings")

spark.stop()