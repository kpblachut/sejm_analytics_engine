from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, count, when, greatest, round

spark = SparkSession.builder.appName("GovBuildGold").master("local[*]").getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# df_mps = spark.read.parquet("../data/silver/mps")
df_votes = spark.read.parquet("../data/silver/votes")
# df_votes_on_list = spark.read.parquet("../data/silver/votes_on_list")
# df_voting_meta = spark.read.parquet("../data/silver/voting_meta")

# df_votings_enriched = df_votings.join(
#     df_mps,
#     ["term", "mp_id"],
#     "left"
# )

# df_votings_enriched.write \
#     .mode("overwrite") \
#     .option("compression", "snappy") \
#     .partitionBy("term") \
#     .parquet("../data/gold/votes_enriched")

df_votes.printSchema()

df_votes.groupBy(
    "club_at_vote",
    "term",
    "sitting",
    "vote_id"
).agg(
    count(when(col("vote_normalized") == "YES", 1)).alias("yes_count"),
    count(when(col("vote_normalized") == "NO", 1)).alias("no_count"),
    count(when(col("vote_normalized") == "ABSTAIN", 1)).alias("abstain_count"),
    count(when(col("vote_normalized") == "ABSENT", 1)).alias("absent_count"),
    count(when(col("vote_normalized") == "OTHER", 1)).alias("other_count"),
    count(when(col("did_vote"), 1)).alias("present_count"),
    count("*").alias("total_count")
).withColumn(
        "max_votes",
        greatest(
            col("yes_count"),
            col("no_count"),
            col("abstain_count")
        )
).withColumn(
    "party_cohesion",
    round(col("max_votes") / col("present_count") * 100, 2)
).withColumn(
        "n_max_categories",
        when(col("yes_count") == col("max_votes"), 1).otherwise(0) +
        when(col("no_count") == col("max_votes"), 1).otherwise(0) +
        when(col("abstain_count") == col("max_votes"), 1).otherwise(0)
).withColumn(
        "party_line",
        when(col("n_max_categories") > 1, "TIE")
        .when(col("yes_count") == col("max_votes"), "YES")
        .when(col("no_count") == col("max_votes"), "NO")
        .when(col("abstain_count") == col("max_votes"), "ABSTAIN")
) \
.show(20, truncate=False)
