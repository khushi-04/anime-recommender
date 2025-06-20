# pre processing -- cleans data by filling in missing values, converting and solidifying data types, prints summary stats

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, when
from pyspark.sql.types import NumericType, StringType
from pyspark.ml.feature import VectorAssembler, MinMaxScaler, RegexTokenizer
from pyspark.ml import Pipeline
import sqlite3 

spark = SparkSession.builder.appName("CleanCSV").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.csv("final_animedataset.csv", header=True, inferSchema=True)

numeric_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, NumericType)]
string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]

# clean numeric columns
for c in numeric_cols:
    df = df.withColumn(c, when(lower(trim(col(c).cast("string"))) == "unknown", 0).when(col(c).isNull(), 0).otherwise(col(c)))

# clean string columns
for c in string_cols:
    df = df.withColumn(c, when(col(c).isNull(), "unknown").otherwise(col(c)))

df = df.withColumn("score", col("score").cast("float"))

# ml pipeline
assembler = VectorAssembler(inputCols=numeric_cols, outputCol="numFeatures")
scaler = MinMaxScaler(inputCol="numFeatures", outputCol="scaledFeatures")
tokenizer = RegexTokenizer(inputCol="genre", outputCol="genreTokens", pattern="\\W+")

pipeline = Pipeline(stages=[assembler, scaler, tokenizer])
model = pipeline.fit(df)
df_final = model.transform(df)


df_final.select(*df.columns, "scaledFeatures", "genreTokens").coalesce(1)

df_to_write = df_final \
  .withColumn("numFeatures",    col("numFeatures").cast(StringType())) \
  .withColumn("scaledFeatures", col("scaledFeatures").cast(StringType())) \
  .withColumn("genreTokens",    col("genreTokens").cast(StringType()))

df_to_write.coalesce(1).write.csv("animedataset_processed.csv", header=True, mode="overwrite")

# getting stats
df_processed = spark.read.csv("animedataset_processed.csv", header=True, inferSchema=True)

df_processed.show(5)
print("Row count:", df_processed.count())
for c in df_processed.columns:
    df_processed.select(c).describe().show()

print("Number of rows:", df_processed.count())
print("Number of columns:", len(df_processed.columns))
print("Column names:", df_processed.columns)

df_processed.printSchema()

spark.stop()