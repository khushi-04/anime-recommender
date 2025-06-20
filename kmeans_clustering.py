from pyspark.sql import SparkSession #starts spark job
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler, StandardScaler #vec -> combines col into a single vector col, scaler -> scales the vector col
from pyspark.ml.clustering import KMeans
from io import StringIO
import sys

output_filename = "kmeans_output.txt"

original_stdout = sys.stdout
buffer = StringIO()

spark = SparkSession.builder \
    .appName("Anime KMeans Clustering") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.csv(
    "/home/cs179g/animedataset_processed.csv/part-00000-685a087d-d436-4278-aec2-ee82b77c491d-c000.csv",
    header=True,
    inferSchema=True
)
#filters out any row where at least one of the columns is null
df = df.dropna(subset=["my_score", "score", "scored_by", "rank", "popularity"])

#combines all numeric columns into a single features column so we can run Kmeans on it
assembler = VectorAssembler(
    inputCols=["my_score", "score", "scored_by", "rank", "popularity"],
    outputCol="features"
)
assembled_df = assembler.transform(df)

scaler = StandardScaler(inputCol="features", outputCol="scaled_features", withStd=True, withMean=False)
scaler_model = scaler.fit(assembled_df)
scaled_df = scaler_model.transform(assembled_df)

# model
k = 5
kmeans = KMeans(featuresCol="scaled_features", predictionCol="cluster", k=k)
model = kmeans.fit(scaled_df)
clustered_df = model.transform(scaled_df)

sys.stdout = buffer

#tested part sof the file
print("=== Schema ===")
df.printSchema() #print schema to make ensure corectness 

print("\n=== First 5 Rows ===")
df.show(5, truncate=False) #top 5 rows

print("\n=== Scaled Feature Vectors (first 5) ===")
scaled_df.select("scaled_features").show(5, truncate=False)

print("\n=== Clustered Output (first 10 rows) ===")
clustered_df.select("my_score", "score", "scored_by", "rank", "popularity", "cluster").show(10)

print("\n=== Distinct Clusters ===")
clustered_df.select("cluster").distinct().show() #show num of clusters (0 to 4)

print("\n=== Average of Each Column per Cluster ===")
clustered_df.groupBy("cluster").mean("my_score", "score", "scored_by", "rank", "popularity").show()

clustered_df.select("anime_id","my_score", "score", "scored_by", "rank", "popularity", "cluster") \
    .coalesce(1) \
    .write \
    .format("jdbc") \
    .option("url", "jdbc:sqlite:/home/cs179g/kmeans_clusters.db") \
    .option("dbtable", "clusters") \
    .option("driver", "org.sqlite.JDBC") \
    .mode("overwrite") \
    .save()

sys.stdout = original_stdout
with open(output_filename, "w") as f:
    f.write(buffer.getvalue())

spark.stop() 
