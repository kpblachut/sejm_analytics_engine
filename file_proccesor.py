from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, input_file_name, regexp_extract, collect_list, sha2, concat, lit, count_distinct, expr, struct

spark = SparkSession.builder.appName("GovTransform").master("local[*]").getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df_mps_raw = spark.read.format("json").option("recursiveFileLookup", "true").option("multiLine", "true").load("./data/raw/mps/") \
    .withColumn(
        "fileName",
        input_file_name()
    ) \
    .withColumn(
        "term",
        regexp_extract("fileName", r"term(\d+)", 1)
    ) \
    .withColumn(
        "mpIdPrehash",
        concat(col("firstName"), lit("_"), col("lastName"), lit("_"), col("birthDate"), lit("_"), col("birthLocation"))
    ) \
    .withColumn(
        "mpId",
        sha2(col("mpIdPrehash"), 256)
    )

# df_mps_raw.printSchema()
# print(df_mps_raw.count())
# df_mps_raw.select(count_distinct("mpId")).show()
# df_mps_raw.select("term", "mpId", "mpIdPrehash").where(expr("term == 7")).show(10)
# df_mps_raw.select("firstName", "lastName", "mpId", "term").groupBy("mpId").agg(collect_list("term").alias("terms")).show(5,False)
# df_mps_raw.select("firstName", "lastName").filter(expr("mpId == '005b3e203742115fab2eb792775e50272c3dc0c4d4c7f793c41a2dc8eb79dfe4'")).show(truncate=False)


df_mps_transformed = df_mps_raw \
    .groupBy("mpId", "firstName", "lastName") \
    .agg(
        collect_list(col("term")).alias("terms"),
        collect_list(struct(col("term"), col("club"))).alias("clubs"),
        collect_list(struct(col("term"), col("districtName"))).alias("districtNames"),
        collect_list(struct(col("term"), col("districtNum"))).alias("districtNums"),
        collect_list(struct(col("term"), col("educationLevel"))).alias("educationLevels"),
        collect_list(struct(col("term"), col("numberOfVotes"))).alias("numberOfVotesPerTerm"),
        collect_list(struct(col("term"), col("voivodeship"))).alias("voivodeships"),
        collect_list(struct(col("term"), col("id"))).alias("ids"),
        collect_list(struct(col("term"), col("profession"))).alias("proffesions")
    )


df_votings_raw = spark.read.format("json").option("recursiveFileLookup", "true").option("multiLine", "true").load("./data/raw/votings/7/proceeding/") \
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


# df_mps_transformed.filter(expr("size(ids) >= 2")).show(10, False)
# df_raw = spark.read.format("json").option("multiLine", "true").load("./data/raw/votings/7/proceeding/").withColumn("file_name", input_file_name()).withColumn(
#         "term", 
#         regexp_extract("file_name", r"term(\d+)", 1)) \
#     .withColumn(
#         "proceeding",
#         regexp_extract("file_name",r"proceeding(\d+)", 1)) \
#     .withColumn(
#         "vote_no", 
#         regexp_extract("file_name", r"vote(\d+)", 1))
# df_raw.printSchema()
# df_raw.groupBy("MP").agg(collect_list("vote").alias("votes")).show(10)

# df_votings = spark.read.option("recursiveFileLookup", "true").json("./data/raw/votings/")
# df_mps = spark.read.option("recursiveFileLookup", "true").json("./data/raw/mps/")

# df_votings = df_raw.withColumn("vote_data", explode(col("votes"))).select(
#     col("term"),
#         col("sitting"),
#         col("sittingDay"),
#         col("date"),
#         col("title"),
#         col("description"),
#         col("kind"),
#         col("totalVoted"),
#         col("yes"), 
#         col("no"),
#         col("abstain"),
#         col("vote_data.MP").alias("mp_id"),
#         col("vote_data.firstName").alias("first_name"),
#         col("vote_data.lastName").alias("last_name"),
#         col("vote_data.club").alias("club"),
#         col("vote_data.vote").alias("vote"),
# )

# df_votings.printSchema()
# df_votings.show(5, truncate=False)

# print("Voting Records Count: ", df_votings.count())