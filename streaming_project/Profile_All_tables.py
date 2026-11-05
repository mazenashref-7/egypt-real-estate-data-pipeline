import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, isnan, when, countDistinct

# إنشاء جلسة Spark
spark = SparkSession.builder \
    .appName("ComprehensiveDataProfiling") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
    .getOrCreate()

db_url = "jdbc:postgresql://postgres:5432/realestate"
db_properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

tables = [
    "bronze.raw_egypt_inflation_monthly_2025_2026",
    "bronze.raw_egypt_locations",
    "bronze.raw_egypt_real_estate_listings",
    "bronze.raw_listings_stream",
    "bronze.raw_propertyfinder",
    "bronze.raw_usd_egp_monthly",
    "bronze.stg_listings"
]

# تحديد مسار ملف التقرير (هيظهر عندك في مجلد المشروع في VS Code)
report_file = "/app/profiling_report.txt"

# فتح الملف للكتابة وتوجيه المخرجات ليه
with open(report_file, "w", encoding="utf-8") as f:
    # حفظ مخرج الشاشة الأصلي
    original_stdout = sys.stdout 
    # توجيه الطباعة للملف
    sys.stdout = f 

    for table in tables:
        print(f"\n{'='*60}")
        print(f" Comprehensive Profiling for Table: {table} ")
        print(f"{'='*60}")
        
        try:
            df = spark.read.jdbc(url=db_url, table=table, properties=db_properties)
            
            # 1. عدد الصفوف والأعمدة
            total_rows = df.count()
            total_cols = len(df.columns)
            print(f"Total Rows: {total_rows}")
            print(f"Total Columns: {total_cols}")
            
            # 2. فحص التكرار
            distinct_rows = df.dropDuplicates().count()
            duplicate_rows = total_rows - distinct_rows
            print(f"Duplicate Rows: {duplicate_rows} ({(duplicate_rows/total_rows)*100:.2f}% of data)" if total_rows > 0 else "Duplicate Rows: 0")
            
            # 3. هيكل البيانات
            print("\n--- Schema ---")
            df.printSchema()
            
            # 4. حساب القيم الفارغة
            print("\n--- Missing Values (Nulls / NaNs) ---")
            missing_exprs = []
            for c, t in df.dtypes:
                if t in ("double", "float"):
                    missing_exprs.append(count(when(isnan(c) | col(c).isNull(), c)).alias(c))
                else:
                    missing_exprs.append(count(when(col(c).isNull(), c)).alias(c))
            df.select(missing_exprs).show(vertical=True)
            
            # 5. حساب القيم الفريدة
            print("\n--- Distinct Values Count ---")
            distinct_exprs = [countDistinct(c).alias(c) for c in df.columns]
            df.select(distinct_exprs).show(vertical=True)
            
            # 6. ملخص إحصائي شامل
            print("\n--- Comprehensive Summary Statistics ---")
            df.summary().show()
            
        except Exception as e:
            print(f"Failed to profile {table}. Error: {e}")

    # إرجاع الطباعة للشاشة مرة تانية بعد انتهاء الحلقة
    sys.stdout = original_stdout

print(f"\n✅ Profiling is complete! Please open '{report_file}' in VS Code to see the full report.")

spark.stop()