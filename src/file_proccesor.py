from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    count_distinct, explode, input_file_name, regexp_extract, col, sha2,
    concat_ws, lower, translate, regexp_replace, when, map_entries,
    to_json, from_json
)
from pyspark.sql.types import MapType, StringType

spark = SparkSession.builder.appName("GovTransform").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")


def normalize_name(c):
    c = lower(c)
    c = translate(c, "ąćęłńóśźż", "acelnoszz")
    c = regexp_replace(c, r"[^a-z ]", "")
    c = regexp_replace(c, r"\s+", " ")
    return c


def normalize_vote(vote_col):
    return when(
        vote_col.isin("YES", "NO", "ABSTAIN", "ABSENT"), vote_col
    ).otherwise("OTHER")


df_mps_raw = spark.read \
    .format("json") \
    .option("recursiveFileLookup", "true") \
    .option("multiLine", "true") \
    .load("../data/raw/mps/") \
    .withColumn("fileName", input_file_name()) \
    .withColumn("term", regexp_extract("fileName", r"term(\d+)", 1).cast("int"))

df_mps_clean = df_mps_raw.select(
    sha2(
        concat_ws("|",
            normalize_name(col("firstName")),
            normalize_name(col("lastName")),
            col("birthDate").cast("string")
        ), 256
    ).alias("person_id"),
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
    col("term")
)


df_votings_raw = spark.read \
    .format("json") \
    .option("recursiveFileLookup", "true") \
    .option("multiLine", "true") \
    .load("../data/raw/votings/")

df_voting_meta = df_votings_raw.select(
    col("term").cast("int").alias("term"),
    col("sitting").cast("int").alias("sitting"),
    col("votingNumber").cast("int").alias("vote_id"),
    col("sittingDay"),
    col("date"),
    col("title"),
    col("topic"),
    col("description"),
    col("kind"),
    col("majorityType"),
    col("majorityVotes"),
    col("yes"),
    col("no"),
    col("abstain"),
    col("notParticipating"),
    col("present"),
    col("totalVoted")
)


df_exploded = df_votings_raw.withColumn("v", explode(col("votes")))

df_votes = df_exploded.select(
    col("term").cast("int").alias("term"),
    col("sitting").cast("int").alias("sitting"),
    col("votingNumber").cast("int").alias("vote_id"),
    col("v.MP").cast("int").alias("mp_id"),
    col("v.club").alias("club_at_vote"),
    col("v.vote").alias("vote_raw"),
    normalize_vote(col("v.vote")).alias("vote_normalized")
).withColumn(
    "did_vote",
    col("vote_normalized").isin("YES", "NO", "ABSTAIN")
)

df_votes_on_list = df_exploded \
    .filter(col("v.listVotes").isNotNull()) \
    .withColumn(
        "list_votes_map",
        from_json(to_json(col("v.listVotes")), MapType(StringType(), StringType()))
    ) \
    .select(
        col("term").cast("int").alias("term"),
        col("sitting").cast("int").alias("sitting"),
        col("votingNumber").cast("int").alias("vote_id"),
        col("v.MP").cast("int").alias("mp_id"),
        col("v.club").alias("club_at_vote"),
        explode(map_entries(col("list_votes_map"))).alias("entry")
    ) \
    .select(
        "term", "sitting", "vote_id", "mp_id", "club_at_vote",
        col("entry.key").cast("int").alias("option_number"),
        col("entry.value").alias("vote_raw"),
        normalize_vote(col("entry.value")).alias("vote_normalized")
    ).withColumn(
        "did_vote",
        col("vote_normalized").isin("YES", "NO", "ABSTAIN")
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
    .parquet("../data/silver/voting_meta")

df_votes.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .partitionBy("term") \
    .parquet("../data/silver/votes")

df_votes_on_list.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .partitionBy("term") \
    .parquet("../data/silver/votes_on_list")




# 1. Czy person_id stabilny między kadencjami?
df_mps_clean.groupBy("person_id").agg(
    count_distinct("term").alias("n_terms"),
    count_distinct(concat_ws("|", "first_name", "last_name")).alias("n_names")
).filter(col("n_names") > 1).show()
# Jeśli puste → świetnie. Jeśli coś jest → znaczy że dwie osoby zhashowały się tak samo,
# albo (częściej) ta sama osoba ma inaczej zapisane nazwisko między kadencjami.

# 2. Jakie wartości faktycznie ma vote_raw?
df_votes.select("vote_raw").distinct().show()
df_votes_on_list.select("vote_raw").distinct().show()
# Sprawdź czy nic nie wpadło w "OTHER" niespodziewanie.

# 3. Czy club_at_vote się zmienia w trakcie kadencji?
df_votes.select("term", "mp_id", "club_at_vote").distinct() \
    .groupBy("term", "mp_id").count() \
    .filter(col("count") > 1).show()
# Jeśli coś jest > 1 → masz historię klubową w danych. Bingo.

spark.stop()