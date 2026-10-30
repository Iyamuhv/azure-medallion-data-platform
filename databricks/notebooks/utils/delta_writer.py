# Databricks notebook source
# MAGIC %md
# MAGIC # delta_writer
# MAGIC Handles all Silver write patterns:
# MAGIC   - MERGE      — upsert on business key
# MAGIC   - APPEND     — immutable records
# MAGIC   - FULL_RELOAD — truncate + insert (reference tables)
# MAGIC   - SNAPSHOT   — dual write current + history

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime, timezone
from typing import Optional


def write_merge(df: DataFrame,
                target_table: str,
                business_key: str,
                watermark_field: Optional[str] = None) -> dict:
    """
    Upserts records into a Silver Delta table using MERGE.
    1. Deduplicates on business_key (latest watermark_field wins)
    2. Adds _ingested_at metadata
    3. Executes Delta MERGE (upsert)
    """
    print(f"  [delta_writer] MERGE → {target_table} on {business_key}")

    if df is None or df.limit(1).count() == 0:
        print(f"  [delta_writer] Empty DataFrame — skipping MERGE")
        return {"rows_inserted": 0, "rows_updated": 0, "rows_total": 0}

    # Deduplicate: keep latest record per business key
    if watermark_field and watermark_field in df.columns:
        w = Window.partitionBy(business_key).orderBy(F.col(watermark_field).desc())
        df = df.withColumn("_rn", F.row_number().over(w))                .filter(F.col("_rn") == 1).drop("_rn")

    df = df.withColumn("_ingested_at", F.current_timestamp())

    delta_tbl = DeltaTable.forName(spark, target_table)
    delta_tbl.alias("tgt").merge(
        df.alias("src"),
        f"tgt.{business_key} = src.{business_key}"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    history = delta_tbl.history(1).collect()[0]
    rows_inserted = history["operationMetrics"].get("numTargetRowsInserted", 0)
    rows_updated  = history["operationMetrics"].get("numTargetRowsUpdated", 0)
    rows_total    = df.count()

    print(f"  [delta_writer] MERGE complete: {rows_inserted} inserted, {rows_updated} updated")
    return {"rows_inserted": int(rows_inserted),
            "rows_updated": int(rows_updated),
            "rows_total": rows_total}


def write_full_reload(df: DataFrame, target_table: str) -> dict:
    """
    Truncates target table and inserts all records.
    Uses INSERT OVERWRITE to preserve Delta table properties.
    """
    print(f"  [delta_writer] FULL_RELOAD → {target_table}")

    if df is None or df.limit(1).count() == 0:
        print(f"  [delta_writer] Empty DataFrame — skipping")
        return {"rows_loaded": 0}

    row_count = df.count()
    df = df.withColumn("_ingested_at", F.current_timestamp())

    ddl_cols = [
        row["col_name"]
        for row in spark.sql(f"DESCRIBE TABLE {target_table}").collect()
        if not row["col_name"].startswith("#") and row["data_type"] != ""
    ]
    select_cols = [c for c in ddl_cols if c in df.columns]
    df_aligned  = df.select(select_cols)
    temp_view   = target_table.replace(".", "_") + "_tmp"
    df_aligned.createOrReplaceTempView(temp_view)
    col_list = ", ".join(select_cols)
    spark.sql(f"INSERT OVERWRITE {target_table} ({col_list}) SELECT {col_list} FROM {temp_view}")
    spark.catalog.dropTempView(temp_view)

    print(f"  [delta_writer] FULL_RELOAD complete: {row_count} rows loaded")
    return {"rows_loaded": row_count}


def write_delta(df: DataFrame,
                target_table: str,
                write_pattern: str,
                business_key: Optional[str] = None,
                watermark_field: Optional[str] = None) -> dict:
    """
    Dispatcher: routes to the correct write pattern.
    """
    if write_pattern == "merge":
        if not business_key:
            raise ValueError("business_key required for MERGE pattern")
        return write_merge(df, target_table, business_key, watermark_field)
    elif write_pattern in ("full_reload", "full reload"):
        return write_full_reload(df, target_table)
    elif write_pattern == "append":
        df = df.withColumn("_ingested_at", F.current_timestamp())
        df.write.format("delta").mode("append").saveAsTable(target_table)
        row_count = df.count()
        print(f"  [delta_writer] APPEND complete: {row_count} rows appended")
        return {"rows_appended": row_count}
    else:
        raise ValueError(f"Unknown write_pattern: {write_pattern}. "
                         f"Expected: merge | append | full_reload")


print("delta_writer loaded ✅")
