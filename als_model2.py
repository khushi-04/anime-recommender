# code from part 2 before modifying for taking user input and creating fake user profile

from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.sql.functions import explode 

spark = SparkSession.builder.appName("AnimeALSRecommendation").config("spark.driver.memory", "8g").config("spark.executor.memory", "8g").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
ratings = spark.read.csv("final_animedataset.csv", header=True, inferSchema=True)

ratings_clean = ratings.select("user_id", "anime_id", "my_score").na.drop().filter("my_score > 0")

als = ALS(
    userCol="user_id",
    itemCol="anime_id",
    ratingCol="my_score",
    coldStartStrategy="drop",
    nonnegative=True,
    rank=10,
    maxIter=10,
    regParam=0.1
)

model = als.fit(ratings_clean)
top_recommendations = model.recommendForAllUsers(5)

exploded = top_recommendations.withColumn("rec", explode("recommendations")).select("user_id", "rec.anime_id", "rec.rating")

anime_metadata = ratings.select("anime_id", "title", "genre").dropDuplicates(["anime_id"])
final_recommendations = exploded.join(anime_metadata, on="anime_id", how="left")

kmeans_clusters = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:sqlite:/home/cs179g/kmeans_clusters.db") \
    .option("dbtable", "clusters") \
    .option("driver", "org.sqlite.JDBC") \
    .load() \
    .select("anime_id", "cluster") \
    .dropDuplicates(["anime_id"])

final_combined_recs = final_recommendations.join(kmeans_clusters, on="anime_id", how="left")
final_combined_recs.show(20, truncate=False)

final_combined_recs.coalesce(1) \
    .write \
    .format("jdbc") \
    .option("url", "jdbc:sqlite:/home/cs179g/als_with_clusters.db") \
    .option("dbtable", "user_recommendations") \
    .option("driver", "org.sqlite.JDBC") \
    .mode("overwrite") \
    .save()

final_recommendations.write.csv("output_anime_recommendations", header=True, mode="overwrite")
spark.stop() 
