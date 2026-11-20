from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, initcap, current_timestamp,
    regexp_replace, when, to_date, date_format
)
from pyspark.sql.types import DoubleType, IntegerType, StringType

spark = SparkSession.builder \
    .appName("03_CleanEgyptRealEstate") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
    .getOrCreate()

db_url = "jdbc:postgresql://postgres:5432/realestate"
db_properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

df = spark.read.jdbc(db_url, "bronze.raw_egypt_real_estate_listings", properties=db_properties)

for old_name in df.columns:
    new_name = old_name.strip()
    if old_name != new_name:
        df = df.withColumnRenamed(old_name, new_name)

initial_count = df.count()
df = df.dropDuplicates(["url"]) if "url" in df.columns else df.dropDuplicates()

skip_initcap = ["url", "description", "available_from"]
for field in df.schema.fields:
    c = field.name
    if isinstance(field.dataType, StringType):
        df = df.withColumn(c, trim(col(c)))
        df = df.withColumn(c, when(col(c) == "", None).otherwise(col(c)))
        if c not in skip_initcap:
            df = df.withColumn(c, initcap(col(c)))

for c in ["price", "size"]:
    if c in df.columns:
        cleaned = regexp_replace(col(c).cast(StringType()), "[^0-9.-]", "")
        df = df.withColumn(c, when(cleaned == "", None).otherwise(cleaned).cast(DoubleType()))

if "down_payment" in df.columns:
    dp_clean = regexp_replace(col("down_payment").cast(StringType()), "[^0-9.-]", "")
    df = df.withColumn("down_payment_egp", when(dp_clean == "", None).otherwise(dp_clean).cast(DoubleType()))

for c in ["bedrooms", "bathrooms"]:
    if c in df.columns:
        cleaned = regexp_replace(col(c).cast(StringType()), "[^0-9-]", "")
        df = df.withColumn(c, when(cleaned == "", None).otherwise(cleaned).cast(IntegerType()))

if "available_from" in df.columns:
    df = df.withColumn("available_from_date", to_date(col("available_from"), "d MMM yyyy"))
    df = df.withColumn("listing_year_month", date_format(col("available_from_date"), "yyyy-MM"))

df = df.filter(col("price").isNotNull() & (col("price") > 0))
df = df.filter(col("size").isNull() | (col("size") > 0))

df = df.withColumn("_silver_processed_at", current_timestamp()).cache()
final_count = df.count()

df.write.jdbc(db_url, "silver.egypt_real_estate_listings", mode="overwrite", properties=db_properties)
df.unpersist()
spark.stop()
print(f"✅ Done silver.egypt_real_estate_listings: {initial_count} -> {final_count}")