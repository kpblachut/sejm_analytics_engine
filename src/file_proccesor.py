from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, explode, input_file_name, regexp_extract, col, lit, sha2

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
    ) \
    .withColumn(
        "mp_id_prehash",
        concat(col("firstName"), lit("_"), col("lastName"), lit("_"), col("birthDate"), lit("_"), col("birthLocation"))
    ) \
    .withColumn(
        "person_id",
        sha2(col("mp_id_prehash"), 256)
    )

df_mps_clean = df_mps_raw \
    .select(
        col("person_id"),
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
    .load("../data/raw/votings/")

df_voting_meta = df_votings_raw \
    .select(
        col("abstain"),
        col("date"),
        col("description"),
        col("kind"),
        col("majorityType"),
        col("majorityVotes"),
        col("no"),
        col("notParticipating"),
        col("present"),
        col("sitting"),
        col("sittingDay"),
        col("notParticipating"),
        col("term"),
        col("title"),
        col("topic"),
        col("totalVoted"),
        col("votingNumber"),
        col("yes")
    )

df_votings_exploded = df_votings_raw.withColumn("votes_exploded", explode(col("votes")))

df_votes = df_votings_exploded.select(
    col("votes_exploded.MP").alias("MP"),
    col("votes_exploded.vote").alias("vote"),
    col("term"),
    col("sitting"),
    col("votingNumber")
)

df_mps_clean.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .partitionBy("term") \
    .parquet("../data/silver/mps")

df_voting_meta.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .partitionBy("term") \
    .parquet("../data/silver/votings_meta")

df_votes.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .partitionBy("term") \
    .parquet("../data/silver/votes")

spark.stop()