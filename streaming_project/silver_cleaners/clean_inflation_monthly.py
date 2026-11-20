from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when, current_timestamp
from pyspark.sql.types import DoubleType, StringType

spark = SparkSession.builder \
    .appName("05_CleanEgyptInflationMonthly") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
    .getOrCreate()

db_url = "jdbc:postgresql://postgres:5432/realestate"
db_properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

df = spark.read.jdbc(db_url, "bronze.raw_egypt_inflation_monthly_2025_2026", properties=db_properties)
initial_count = df.count()

df = df.withColumn("year_month", trim(col("year_month").cast(StringType())))
df = df.withColumn("year_month", when(col("year_month") == "", None).otherwise(col("year_month")))
df = df.filter(col("year_month").isNotNull()).dropDuplicates(["year_month"])

pct_cols = ["urban_annual_pct", "urban_mom_pct", "national_annual_pct", "core_annual_pct"]
for c in pct_cols:
    if c in df.columns:
        df = df.withColumn(c, col(c).cast(DoubleType()))

df = df.orderBy("year_month").withColumn("_silver_processed_at", current_timestamp())

final_count = df.count()
df.write.jdbc(db_url, "silver.egypt_inflation_monthly", mode="overwrite", properties=db_properties)
spark.stop()
print(f"✅ Done silver.egypt_inflation_monthly: {initial_count} -> {final_count}")