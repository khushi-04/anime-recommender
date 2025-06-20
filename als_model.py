# 1: takes in user input
# 2: builds als collaborative filtering recommendation system model
# 3: joins recommendations with pre determined k means clusters
# 4: writes the final output (priotizing genres to consider genre as another input) to a sqlite database

import argparse
from pyspark.sql import SparkSession, Row
from pyspark.ml.recommendation import ALS
from pyspark.sql.functions import explode, col
import os, glob, shutil
from pyspark import StorageLevel
from pyspark.sql.functions import broadcast
from pyspark.sql.functions import when
from pyspark.sql.functions import lit

# user inputs
parser = argparse.ArgumentParser()
parser.add_argument('--anime', nargs='*', default=[])
parser.add_argument('--genres', nargs='*', default=[])
args = parser.parse_args()
input_animes = args.anime
input_genres = args.genres

print("Parsed anime titles:", input_animes, flush=True)
print("Parsed genres:", input_genres, flush=True)

spark = SparkSession.builder.appName("AnimeALSRecommendation").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

ratings = spark.read.csv(
    "/home/cs179g/final_animedataset.csv",
    header=True,
    inferSchema=True
)

ratings_clean = (ratings.select("user_id", "anime_id", "my_score").na.drop()
                 .filter("my_score > 0").persist(StorageLevel.MEMORY_AND_DISK_DESER))

anime_metadata = (ratings.select("anime_id", "title", "genre").dropDuplicates(["anime_id"]))

input_df = spark.createDataFrame([(title,) for title in input_animes], ["title"])

matched_animes = input_df.join(anime_metadata, on="title", how="inner")
matched_ids = matched_animes.select("anime_id").rdd.flatMap(lambda x: x).collect()

# creates synthetic user based on user input
synthetic_user_id = -1
synthetic_ratings = spark.createDataFrame([Row(user_id=synthetic_user_id, anime_id=anime_id, my_score=10.0)
    for anime_id in matched_ids])

ratings_clean = ratings_clean.repartition(200, col("user_id"))

ratings_sampled = ratings_clean.sample(withReplacement=False, fraction=0.8, seed=42)

ratings_augmented = ratings_sampled.union(synthetic_ratings)

# build als model
als = ALS(
    userCol="user_id",
    itemCol="anime_id",
    ratingCol="my_score",
    coldStartStrategy="drop",
    nonnegative=True,
    rank=10,
    maxIter=3,
    regParam=0.1
)
model = als.fit(ratings_augmented)

temp_user_df = spark.createDataFrame([(synthetic_user_id,)], ["user_id"])

# get user recommendations for synthetic user
user_recs = (
    model.recommendForUserSubset(temp_user_df, 50)
    .withColumn("rec", explode("recommendations"))
    .select("user_id", "rec.anime_id", "rec.rating")
)

anime_metadata_bc = broadcast(anime_metadata)
final_user_recs = user_recs.join(anime_metadata_bc, on="anime_id", how="left").select("anime_id", "title", "genre", "rating")

if input_animes:
    final_user_recs = final_user_recs.filter(~col("title").isin(input_animes))
final_user_recs = final_user_recs.filter(~col("genre").contains("Hentai"))

# prioritize animes that has input genres in them 
if input_genres:
    gm = col("genre").contains(input_genres[0])
    for g in input_genres[1:]:
        gm = gm | col("genre").contains(g)
    final_user_recs = final_user_recs.withColumn("genre_match", when(gm, lit(1)).otherwise(lit(0)))
else:
    final_user_recs = final_user_recs.withColumn("genre_match", lit(0))

ranked = final_user_recs.orderBy(col("genre_match").desc(), col("rating").desc())

top_recs = ranked.limit(50)

# join with k means clusters
kmeans_clusters = (
    spark.read
    .format("jdbc")
    .option("url", "jdbc:sqlite:/home/cs179g/kmeans_clusters.db")
    .option("dbtable", "clusters")
    .option("driver", "org.sqlite.JDBC")
    .load()
    .select("anime_id", "cluster")
    .dropDuplicates(["anime_id"])
)

kmeans_clusters_bc = broadcast(kmeans_clusters)
final_combined_recs = top_recs.join(
    kmeans_clusters_bc,
    on="anime_id",
    how="left"
)

# write to final recommendation database
output_df = final_combined_recs.select("genre", "title", "cluster")
output_df.coalesce(1).write.format("jdbc") \
    .option("url", "jdbc:sqlite:/home/cs179g/als_with_clusters.db") \
    .option("dbtable", "user_recommendations") \
    .option("driver", "org.sqlite.JDBC") \
    .mode("overwrite") \
    .save()

spark.stop()
