from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, initcap, upper, when, current_timestamp
from pyspark.sql.types import LongType, StringType
from _common import deduplicate_by_key

spark = SparkSession.builder \
    .appName("04_CleanEgyptLocations") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
    .getOrCreate()

db_url = "jdbc:postgresql://postgres:5432/realestate"
db_properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

df = spark.read.jdbc(db_url, "bronze.raw_egypt_locations", properties=db_properties)
initial_count = df.count()

for c in ["city_ar", "city_en", "governorate_code", "governorate_ar", "governorate_en"]:
    if c in df.columns:
        df = df.withColumn(c, trim(col(c).cast(StringType())))
        df = df.withColumn(c, when(col(c) == "", None).otherwise(col(c)))

if "city_en" in df.columns:
    df = df.withColumn("city_en", initcap(col("city_en")))
if "governorate_en" in df.columns:
    df = df.withColumn("governorate_en", initcap(col("governorate_en")))
if "governorate_code" in df.columns:
    df = df.withColumn("governorate_code", upper(col("governorate_code")))

for id_c in ["city_id", "governorate_id"]:
    if id_c in df.columns:
        df = df.withColumn(id_c, col(id_c).cast(LongType()))

df = df.filter(col("city_id").isNotNull())
df = deduplicate_by_key(df, ["city_id"])
df = df.orderBy("governorate_id", "city_id").withColumn("_silver_processed_at", current_timestamp())

final_count = df.count()
df.write.jdbc(db_url, "silver.egypt_locations", mode="overwrite", properties=db_properties)
spark.stop()
print(f"✅ Done silver.egypt_locations: {initial_count} -> {final_count}")