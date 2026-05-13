from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("GovAnalysis").master("local[*]").getOrCreate()

df = spark.read.parquet("../data/gold/votes_enriched")

df.show(30,False)

# df.count()

# df.createOrReplaceTempView("votes")

# attendance = spark.sql("""
# SELECT
#     term,
#     mp_id,
#     first_name,
#     last_name,
#     COUNT(*) AS total_votes,
#     SUM(CASE WHEN vote != 'ABSENT' THEN 1 ELSE 0 END) AS attended
# FROM votes
# GROUP BY
#     term,
#     mp_id,
#     first_name,
#     last_name
# ORDER BY attended ASC
# """)

# attendance.show(20, truncate=False)

spark.stop()