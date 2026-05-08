from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("GovBuildGold").master("local[*]").getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df_votings = spark.read.parquet("../data/silver/votings")
df_mps = spark.read.parquet("../data/silver/mps")

df_votings_enriched = df_votings.join(
    df_mps,
    ["term", "mp_id"],
    "left"
)

df_votings_enriched.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .partitionBy("term") \
    .parquet("../data/gold/votes_enriched")
