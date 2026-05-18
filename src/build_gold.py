from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, greatest, round, avg, sum as spark_sum, lit

spark = SparkSession.builder.appName("GovBuildGold").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df_mps = spark.read.parquet("../data/silver/mps")
df_votes = spark.read.parquet("../data/silver/votes")
df_voting_meta = spark.read.parquet("../data/silver/voting_meta")


df_electronic_meta = df_voting_meta \
    .filter(col("kind") == "ELECTRONIC") \
    .select("term", "sitting", "vote_id", "date", "title", "topic", "kind")

df_votes_filtered = df_votes.join(
    df_electronic_meta.select("term", "sitting", "vote_id"),
    ["term", "sitting", "vote_id"],
    "inner"
)


df_party_lines = df_votes_filtered.groupBy(
    "term", "sitting", "vote_id", "club_at_vote"
).agg(
    count(when(col("vote_normalized") == "YES", 1)).alias("yes_count"),
    count(when(col("vote_normalized") == "NO", 1)).alias("no_count"),
    count(when(col("vote_normalized") == "ABSTAIN", 1)).alias("abstain_count"),
    count(when(col("vote_normalized") == "ABSENT", 1)).alias("absent_count"),
    count(when(col("vote_normalized") == "PRESENT", 1)).alias("present_count"),
    count(when(col("vote_normalized") == "OTHER", 1)).alias("other_count"),
    count(when(col("did_vote"), 1)).alias("voted_count"),
    count("*").alias("total_count")
).withColumn(
    "max_votes",
    greatest(col("yes_count"), col("no_count"), col("abstain_count"))
).withColumn(
    "cohesion_voted",
    when(col("voted_count") > 0, round(col("max_votes") / col("voted_count") * 100, 2))
).withColumn(
    "cohesion_all",
    round(col("max_votes") / col("total_count") * 100, 2)
).withColumn(
    "n_max_categories",
    when(col("yes_count") == col("max_votes"), 1).otherwise(0) +
    when(col("no_count") == col("max_votes"), 1).otherwise(0) +
    when(col("abstain_count") == col("max_votes"), 1).otherwise(0)
).withColumn(
    "party_line_simple",
    when(col("voted_count") == 0, lit(None))
    .when(col("n_max_categories") > 1, "TIE")
    .when(col("yes_count") == col("max_votes"), "YES")
    .when(col("no_count") == col("max_votes"), "NO")
    .when(col("abstain_count") == col("max_votes"), "ABSTAIN")
).withColumn(
    "party_line_strict",
    when(
        (col("cohesion_voted") > 70) & (col("party_line_simple") != "TIE"),
        col("party_line_simple")
    )
)


df_mps_attrs = df_mps.select(
    "term", "mp_id", "person_id", "first_name", "last_name", "birth_date",
    "district_name", "voivodeship", "education_level", "profession",
    "number_of_votes", "club"
).withColumnRenamed("club", "club_end_of_term")


df_loyalty_facts = df_votes_filtered \
    .join(df_mps_attrs, ["term", "mp_id"], "left") \
    .join(df_electronic_meta, ["term", "sitting", "vote_id"], "left") \
    .join(df_party_lines, ["term", "sitting", "vote_id", "club_at_vote"], "left") \
    .select(
        "term", "sitting", "vote_id", "date", "title", "topic",
        "person_id", "mp_id", "first_name", "last_name", "birth_date",
        "club_at_vote", "club_end_of_term",
        "district_name", "voivodeship", "education_level", "profession", "number_of_votes",
        "vote_normalized", "did_vote",
        "party_line_simple", "party_line_strict",
        "cohesion_voted", "cohesion_all"
    ).withColumn(
        "agrees_simple_voted",
        when(col("did_vote") & col("party_line_simple").isNotNull() & (col("party_line_simple") != "TIE"),
             col("vote_normalized") == col("party_line_simple"))
    ).withColumn(
        "agrees_simple_all",
        when(col("party_line_simple").isNotNull() & (col("party_line_simple") != "TIE"),
             col("vote_normalized") == col("party_line_simple")).otherwise(False)
    ).withColumn(
        "agrees_strict_voted",
        when(col("did_vote") & col("party_line_strict").isNotNull(),
             col("vote_normalized") == col("party_line_strict"))
    ).withColumn(
        "agrees_strict_all",
        when(col("party_line_strict").isNotNull(),
             col("vote_normalized") == col("party_line_strict")).otherwise(False)
    )


df_mp_votes = df_votes_filtered.select(
    "term", "sitting", "vote_id", "mp_id", "vote_normalized", "did_vote", "club_at_vote"
).join(df_mps.select("term", "mp_id", "person_id"), ["term", "mp_id"], "left")

df_all_party_lines = df_party_lines.select(
    "term", "sitting", "vote_id",
    col("club_at_vote").alias("evaluated_club"),
    col("party_line_strict").alias("evaluated_line")
).filter(col("evaluated_line").isNotNull())

df_mp_vs_all_parties = df_mp_votes \
    .join(df_all_party_lines, ["term", "sitting", "vote_id"], "inner") \
    .withColumn(
        "agrees",
        when(col("did_vote"), col("vote_normalized") == col("evaluated_line"))
    ) \
    .groupBy("person_id", "term", "club_at_vote", "evaluated_club") \
    .agg(
        count(when(col("agrees"), 1)).alias("n_agree"),
        count(when(col("agrees").isNotNull(), 1)).alias("n_compared"),
        count("*").alias("n_votes_total")
    ) \
    .withColumn(
        "agreement_pct",
        when(col("n_compared") > 0, round(col("n_agree") / col("n_compared") * 100, 2))
    )


df_mp_career = df_loyalty_facts.groupBy(
    "person_id", "term", "mp_id", "first_name", "last_name", "birth_date",
    "voivodeship", "education_level", "profession", "number_of_votes", "club_end_of_term"
).agg(
    count("*").alias("n_votes_total"),
    count(when(col("did_vote"), 1)).alias("n_voted"),
    count(when(col("vote_normalized") == "ABSENT", 1)).alias("n_absent"),
    count(when(col("vote_normalized") == "PRESENT", 1)).alias("n_present_no_vote"),
    round(avg(when(col("agrees_simple_voted").isNotNull(),
                   col("agrees_simple_voted").cast("double"))) * 100, 2).alias("loyalty_simple_voted_pct"),
    round(avg(when(col("agrees_simple_all").isNotNull(),
                   col("agrees_simple_all").cast("double"))) * 100, 2).alias("loyalty_simple_all_pct"),
    round(avg(when(col("agrees_strict_voted").isNotNull(),
                   col("agrees_strict_voted").cast("double"))) * 100, 2).alias("loyalty_strict_voted_pct"),
    round(avg(when(col("agrees_strict_all").isNotNull(),
                   col("agrees_strict_all").cast("double"))) * 100, 2).alias("loyalty_strict_all_pct")
)


df_party_lines.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .partitionBy("term") \
    .parquet("../data/gold/party_lines")

df_loyalty_facts.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .partitionBy("term") \
    .parquet("../data/gold/loyalty_facts")

df_mp_vs_all_parties.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .partitionBy("term") \
    .parquet("../data/gold/mp_vs_all_parties")

df_mp_career.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .partitionBy("term") \
    .parquet("../data/gold/mp_career")

spark.stop()