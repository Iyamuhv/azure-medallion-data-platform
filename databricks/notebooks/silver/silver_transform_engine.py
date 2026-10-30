# Databricks notebook source
# MAGIC %md
# MAGIC # silver_transform_engine
# MAGIC Single parameterised notebook replacing N per-source notebooks.
# MAGIC Reads field mappings from control table at runtime.
# MAGIC Usage: dbutils.widgets.text("pipeline_name", "silver_crm_cases")

from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, timezone
import json

# ── Widget ────────────────────────────────────────────────────
dbutils.widgets.text("pipeline_name", "", "Pipeline name")
PIPELINE = dbutils.widgets.get("pipeline_name").strip()
if not PIPELINE:
    raise ValueError("pipeline_name widget is required")

print("=" * 60)
print(f"  silver_transform_engine: {PIPELINE}")
print("=" * 60)

# ── Load field mappings from control table ────────────────────
mappings_df = spark.sql(f"""
    SELECT source_field, target_field, data_type,
           transformation, is_business_key, is_dq_checked,
           dq_rule, dq_flag_label
    FROM client_dev.silver.field_mappings
    WHERE pipeline_name = '{PIPELINE}'
    AND is_active = TRUE
    ORDER BY source_field
""")

mappings = [row.asDict() for row in mappings_df.collect()]
if not mappings:
    raise ValueError(f"No active mappings found for pipeline: {PIPELINE}")

print(f"  Loaded {len(mappings)} field mappings")

# ── Load watermark ────────────────────────────────────────────
wm_row = spark.sql(f"""
    SELECT last_watermark, source_bronze_table, target_table,
           write_pattern, merge_key, watermark_field
    FROM client_dev.silver.pipeline_watermarks
    WHERE pipeline_name = '{PIPELINE}'
""").first()

if not wm_row:
    raise ValueError(f"No watermark record for pipeline: {PIPELINE}")

last_watermark    = wm_row["last_watermark"]
bronze_source     = wm_row["source_bronze_table"]
target_table      = wm_row["target_table"]
write_pattern     = wm_row["write_pattern"]
merge_key         = wm_row["merge_key"]
watermark_field   = wm_row["watermark_field"]

print(f"  Bronze source:  {bronze_source}")
print(f"  Target table:   {target_table}")
print(f"  Write pattern:  {write_pattern}")
print(f"  Last watermark: {last_watermark}")

# ── Read Bronze ───────────────────────────────────────────────
BRONZE_PATH = f"abfss://bronze@<storage_account>.dfs.core.windows.net/{bronze_source}/"
df_raw = spark.read.option("recursiveFileLookup", "true").json(BRONZE_PATH)
print(f"  Bronze records: {df_raw.count():,}")

# ── Apply field mappings ──────────────────────────────────────
TYPE_MAP = {
    "STRING":    StringType(),
    "INTEGER":   IntegerType(),
    "LONG":      LongType(),
    "DOUBLE":    DoubleType(),
    "BOOLEAN":   BooleanType(),
    "TIMESTAMP": TimestampType(),
    "DATE":      DateType(),
}

df_silver = df_raw
for m in mappings:
    src  = m["source_field"]
    tgt  = m["target_field"]
    dtype = m["data_type"].upper()

    if src not in df_raw.columns:
        df_silver = df_silver.withColumn(tgt, F.lit(None).cast(TYPE_MAP.get(dtype, StringType())))
        continue

    col_expr = F.col(src)
    if dtype == "TIMESTAMP":
        col_expr = F.to_timestamp(col_expr)
    elif dtype == "DATE":
        col_expr = F.to_date(col_expr)
    elif dtype in TYPE_MAP:
        col_expr = col_expr.cast(TYPE_MAP[dtype])

    # Apply transformations
    if m["transformation"] == "TRIM_NULLIF":
        col_expr = F.when(F.trim(col_expr) == "", None).otherwise(F.trim(col_expr))
    elif m["transformation"] == "UPPER":
        col_expr = F.upper(col_expr)

    df_silver = df_silver.withColumn(tgt, col_expr)

# Select only mapped target columns
target_cols = [m["target_field"] for m in mappings]
df_silver = df_silver.select(*target_cols)

# ── Data Quality checks ───────────────────────────────────────
dq_rules = [m for m in mappings if m["is_dq_checked"]]
dq_results = []

for rule in dq_rules:
    field    = rule["target_field"]
    dq_type  = rule["dq_rule"]
    label    = rule["dq_flag_label"]

    if dq_type == "NOT_NULL":
        fail_count = df_silver.filter(F.col(field).isNull()).count()
    elif dq_type == "POSITIVE":
        fail_count = df_silver.filter(F.col(field) <= 0).count()
    else:
        fail_count = 0

    total = df_silver.count()
    dq_results.append({
        "rule": label, "field": field, "failures": fail_count,
        "pass_rate": round((total - fail_count) / total * 100, 2) if total > 0 else 100
    })
    print(f"  DQ [{label}] on [{field}]: {fail_count} failures ({total - fail_count} passed)")

# ── Write to Silver ───────────────────────────────────────────
%run ./delta_writer

result = write_delta(
    df         = df_silver,
    target_table = target_table,
    write_pattern = write_pattern,
    business_key  = merge_key,
    watermark_field = watermark_field
)

# ── Update watermark ──────────────────────────────────────────
new_watermark = df_silver.agg(F.max(watermark_field)).first()[0] if watermark_field else None

spark.sql(f"""
    UPDATE client_dev.silver.pipeline_watermarks
    SET last_watermark   = '{new_watermark}',
        last_run_status  = 'Success',
        rows_copied      = {result.get("rows_total", result.get("rows_loaded", 0))},
        updated_at       = CURRENT_TIMESTAMP()
    WHERE pipeline_name  = '{PIPELINE}'
""")

print(f"  Watermark updated to: {new_watermark}")
print("=" * 60)
print(f"  ✅ {PIPELINE} complete")
print("=" * 60)
