from pyspark.sql import Window
from pyspark.sql.functions import col, row_number, sha2, struct, to_json


def deduplicate_by_key(df, key_columns, recency_columns=()):
    missing_keys = [name for name in key_columns if name not in df.columns]
    if missing_keys:
        raise ValueError(f"Missing deduplication key columns: {missing_keys}")

    hash_column = "_silver_dedup_hash"
    rank_column = "_silver_dedup_rank"
    if hash_column in df.columns or rank_column in df.columns:
        raise ValueError("Input already contains a reserved deduplication column")

    order_columns = [
        col(name).desc_nulls_last()
        for name in recency_columns
        if name in df.columns
    ]
    record_hash = sha2(to_json(struct(*[col(name) for name in df.columns])), 256)
    ranked = df.withColumn(hash_column, record_hash)
    order_columns.append(col(hash_column).asc())

    window = Window.partitionBy(*key_columns).orderBy(*order_columns)
    return (
        ranked.withColumn(rank_column, row_number().over(window))
        .filter(col(rank_column) == 1)
        .drop(rank_column, hash_column)
    )
