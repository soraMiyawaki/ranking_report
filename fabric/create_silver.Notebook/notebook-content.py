# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a97c725f-e62b-42a6-81be-79df51bc6de5",
# META       "default_lakehouse_name": "lh_ec_bronze",
# META       "default_lakehouse_workspace_id": "24b23f62-544e-4c50-b957-cbf70bb32ba6",
# META       "known_lakehouses": [
# META         {
# META           "id": "a97c725f-e62b-42a6-81be-79df51bc6de5"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Silverテーブルを lh_ec_silver に作成する。既存テーブルの列定義は変更しない。
spark.conf.set("spark.sql.session.timeZone", "UTC")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Yahoo!ショッピング検索語ランキング。1行＝1回の取得（run_id）における1順位。
spark.sql("""
CREATE TABLE IF NOT EXISTS lh_ec_silver.dbo.silver_yahoo_keyword_rank (
    batch_id STRING,
    run_id STRING,
    observed_at TIMESTAMP,
    source_updated_at_raw STRING,
    source_period_start_raw STRING,
    source_period_end_raw STRING,
    rank INT,
    keyword STRING,
    previous_rank INT,
    direction STRING,
    processed_at TIMESTAMP,
    source_updated_date DATE,
    source_period_start_date DATE,
    source_period_end_date DATE
) USING DELTA
""")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 楽天市場商品ランキング（100位まで）。1行＝1回の取得（run_id）における1順位。
spark.sql("""
CREATE TABLE IF NOT EXISTS lh_ec_silver.dbo.silver_rakuten_item_rank (
    batch_id STRING,
    run_id STRING,
    observed_at TIMESTAMP,
    source_updated_at_raw STRING,
    rank INT,
    item_code STRING,
    item_name STRING,
    item_price BIGINT,
    shop_code STRING,
    shop_name STRING,
    genre_id STRING,
    review_count BIGINT,
    review_average DOUBLE,
    point_rate INT,
    postage_flag INT,
    availability INT,
    item_url STRING,
    processed_at TIMESTAMP,
    source_updated_at TIMESTAMP
) USING DELTA
""")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 列追加（2026-10-10）：提供元日時の解釈済み列。作成済みテーブルにも無ければ追加する。
# Yahoo：日付のみ（タイムゾーン表記なし）。日本の暦日としてDATEで保持する。
# 楽天：RFC 1123形式（例 "Fri, 09 Oct 2026 00:00:00 +0900"）。UTCのTIMESTAMPで保持する。
NEW_COLUMNS = {
    "lh_ec_silver.dbo.silver_yahoo_keyword_rank": [
        ("source_updated_date", "DATE"),
        ("source_period_start_date", "DATE"),
        ("source_period_end_date", "DATE"),
    ],
    "lh_ec_silver.dbo.silver_rakuten_item_rank": [
        ("source_updated_at", "TIMESTAMP"),
    ],
}
for table, columns in NEW_COLUMNS.items():
    existing = set(spark.table(table).columns)
    missing = [f"{name} {dtype}" for name, dtype in columns if name not in existing]
    if missing:
        spark.sql(f"ALTER TABLE {table} ADD COLUMNS ({', '.join(missing)})")
        print(f"{table}: 追加 {missing}")
    else:
        print(f"{table}: 追加なし")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
