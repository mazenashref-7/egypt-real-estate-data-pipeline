from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, regexp_replace, when, current_timestamp
from pyspark.sql.types import DoubleType, StringType
from _common import deduplicate_by_key

spark = SparkSession.builder \
    .appName("06_CleanUsdEgpMonthly") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
    .getOrCreate()

db_url = "jdbc:postgresql://postgres:5432/realestate"
db_properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

df = spark.read.jdbc(db_url, "bronze.raw_usd_egp_monthly", properties=db_properties)
initial_count = df.count()

df = df.withColumn("year_month", trim(col("year_month").cast(StringType())))
df = df.withColumn("year_month", when(col("year_month") == "", None).otherwise(col("year_month")))

cleaned_rate = regexp_replace(col("usd_egp").cast(StringType()), "[^0-9.-]", "")
df = df.withColumn("usd_egp", when(cleaned_rate == "", None).otherwise(cleaned_rate).cast(DoubleType()))

df = df.filter(
    col("year_month").rlike(r"^\d{4}-(0[1-9]|1[0-2])$")
    & col("usd_egp").isNotNull()
    & (col("usd_egp") > 0)
)
df = deduplicate_by_key(df, ["year_month"]).orderBy("year_month")
df = df.withColumn("_silver_processed_at", current_timestamp())

final_count = df.count()
df.write.jdbc(db_url, "silver.usd_egp_monthly", mode="overwrite", properties=db_properties)
spark.stop()
print(f"✅ Done silver.usd_egp_monthly: {initial_count} -> {final_count}")