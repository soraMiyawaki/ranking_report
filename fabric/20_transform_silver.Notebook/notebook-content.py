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

# Bronzeの成功済み取得（SUCCESS かつ bronze_stored）のうち、Silver未処理のrunだけを変換する。
# テーブルは作成しない（create_silverで作成済みであること）。
BRONZE = "lh_ec_bronze.dbo"
SILVER = "lh_ec_silver.dbo"
RAKUTEN_MAX_RANK = 100
spark.conf.set("spark.sql.session.timeZone", "UTC")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable
from pyspark.sql import functions as F

YAHOO_TABLE = f"{SILVER}.silver_yahoo_keyword_rank"
RAKUTEN_TABLE = f"{SILVER}.silver_rakuten_item_rank"
for table in [YAHOO_TABLE, RAKUTEN_TABLE]:
    if not spark.catalog.tableExists(table):
        raise RuntimeError(f"{table}が未作成です。create_silverを先に実行してください。")


def pending_responses(source, bronze_table, silver_table, parsed_column):
    """Silverへ未処理の成功run（解釈済み日時が未設定のrunを含む）について、HTTP 200の応答ページを返す。"""
    done = (spark.table(silver_table)
            .where(F.col(parsed_column).isNotNull() | F.col("source_updated_at_raw").isNull())
            .select("run_id").distinct())
    runs = (spark.table(f"{BRONZE}.ops_collection_run")
            .where((F.col("source") == source) & (F.col("status") == "SUCCESS") & F.col("bronze_stored"))
            .select("run_id")
            .join(done, "run_id", "left_anti"))
    return (spark.table(f"{BRONZE}.{bronze_table}")
            .where(F.col("http_status") == 200)
            .join(runs, "run_id"))


def parse_failed(df, pairs):
    """原文があるのに解釈できなかった日時を返す。"""
    cond = None
    for raw, parsed in pairs:
        c = F.col(raw).isNotNull() & F.col(parsed).isNull()
        cond = c if cond is None else (cond | c)
    return df.where(cond).select(*[raw for raw, _ in pairs]).distinct().limit(5).collect()


def merge_silver(table, df):
    """(run_id, rank) の重複・欠損を検査してからMERGEする。"""
    if df.where(F.col("rank").isNull()).limit(1).count():
        raise RuntimeError(f"{table}: 順位が取得できない行があります。応答の形式を確認してください。")
    dup = df.groupBy("run_id", "rank").count().where("count > 1")
    if dup.limit(1).count():
        raise RuntimeError(f"{table}: 同じrun内で順位が重複しています: {dup.limit(5).collect()}")
    columns = spark.table(table).columns
    df = df.withColumn("processed_at", F.current_timestamp()).select(*columns)
    (DeltaTable.forName(spark, table).alias("t")
        .merge(df.alias("s"), "t.run_id = s.run_id AND t.rank = s.rank")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())


def report(table, df, max_rank):
    """runごとの件数と、1..max_rankで欠けている順位を表示する（欠けていても失敗にはしない）。"""
    for r in df.groupBy("run_id").agg(F.count("*").alias("n"), F.collect_set("rank").alias("ranks")).collect():
        missing = sorted(set(range(1, max_rank + 1)) - set(r["ranks"]))
        print(f"{table}: run_id={r['run_id']} rows={r['n']} missing_ranks={missing[:20]}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Yahoo：検索語ランキングを1行1順位に展開する。数値はSTRINGで読み、明示的に変換する。
YAHOO_SCHEMA = ("STRUCT<keyword_ranking:STRUCT<ranking_data:ARRAY<"
                "STRUCT<rank:STRING,query:STRING,pre_rank:STRING,vector:STRING>>>>")
yahoo = (pending_responses("yahoo", "bronze_yahoo_api_response", YAHOO_TABLE, "source_updated_date")
         .withColumn("data", F.from_json("response_body", YAHOO_SCHEMA))
         .select("batch_id", "run_id", "observed_at", "source_updated_at_raw",
                 "source_period_start_raw", "source_period_end_raw",
                 F.explode("data.keyword_ranking.ranking_data").alias("item"))
         .select("batch_id", "run_id", "observed_at", "source_updated_at_raw",
                 "source_period_start_raw", "source_period_end_raw",
                 F.col("item.rank").cast("int").alias("rank"),
                 F.col("item.query").alias("keyword"),
                 F.col("item.pre_rank").cast("int").alias("previous_rank"),
                 F.col("item.vector").alias("direction"))
         # 日付のみでタイムゾーン表記がないため、日本の暦日としてDATEで保持する。
         .withColumn("source_updated_date", F.to_date("source_updated_at_raw", "yyyy-MM-dd"))
         .withColumn("source_period_start_date", F.to_date("source_period_start_raw", "yyyy-MM-dd"))
         .withColumn("source_period_end_date", F.to_date("source_period_end_raw", "yyyy-MM-dd"))
         .cache())
failed = parse_failed(yahoo, [("source_updated_at_raw", "source_updated_date"),
                              ("source_period_start_raw", "source_period_start_date"),
                              ("source_period_end_raw", "source_period_end_date")])
if failed:
    raise RuntimeError(f"Yahooの日付を解釈できません: {failed}")
report(YAHOO_TABLE, yahoo, 100)
merge_silver(YAHOO_TABLE, yahoo)
yahoo.unpersist()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 楽天：商品ランキングを1行1順位に展開し、RAKUTEN_MAX_RANK位までに絞る。
RAKUTEN_FIELDS = ["rank", "itemCode", "itemName", "itemPrice", "shopCode", "shopName", "genreId",
                  "reviewCount", "reviewAverage", "pointRate", "postageFlag", "availability", "itemUrl"]
RAKUTEN_SCHEMA = "STRUCT<Items:ARRAY<STRUCT<" + ",".join(f"{f}:STRING" for f in RAKUTEN_FIELDS) + ">>>"
rakuten = (pending_responses("rakuten", "bronze_rakuten_api_response", RAKUTEN_TABLE, "source_updated_at")
           .withColumn("data", F.from_json("response_body", RAKUTEN_SCHEMA))
           .select("batch_id", "run_id", "observed_at", "source_updated_at_raw",
                   F.explode("data.Items").alias("item"))
           .select("batch_id", "run_id", "observed_at", "source_updated_at_raw",
                   F.col("item.rank").cast("int").alias("rank"),
                   F.col("item.itemCode").alias("item_code"),
                   F.col("item.itemName").alias("item_name"),
                   F.col("item.itemPrice").cast("bigint").alias("item_price"),
                   F.col("item.shopCode").alias("shop_code"),
                   F.col("item.shopName").alias("shop_name"),
                   F.col("item.genreId").alias("genre_id"),
                   F.col("item.reviewCount").cast("bigint").alias("review_count"),
                   F.col("item.reviewAverage").cast("double").alias("review_average"),
                   F.col("item.pointRate").cast("int").alias("point_rate"),
                   F.col("item.postageFlag").cast("int").alias("postage_flag"),
                   F.col("item.availability").cast("int").alias("availability"),
                   F.col("item.itemUrl").alias("item_url"))
           .where(F.col("rank").isNull() | (F.col("rank") <= RAKUTEN_MAX_RANK))
           # RFC 1123形式（例 "Fri, 09 Oct 2026 00:00:00 +0900"）。曜日を除いて解釈し、UTCで保持する。
           .withColumn("source_updated_at", F.to_timestamp(
               F.regexp_replace("source_updated_at_raw", r"^[A-Za-z]{3}, ", ""), "dd MMM yyyy HH:mm:ss Z"))
           .cache())
failed = parse_failed(rakuten, [("source_updated_at_raw", "source_updated_at")])
if failed:
    raise RuntimeError(f"楽天のlastBuildDateを解釈できません: {failed}")
report(RAKUTEN_TABLE, rakuten, RAKUTEN_MAX_RANK)
merge_silver(RAKUTEN_TABLE, rakuten)
rakuten.unpersist()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 処理結果の確認：Silverのrunごとの件数。
for table in [YAHOO_TABLE, RAKUTEN_TABLE]:
    print(table)
    updated = "source_updated_date" if table == YAHOO_TABLE else "source_updated_at"
    display(spark.table(table).groupBy("batch_id", "run_id", "source_updated_at_raw", updated)
            .agg(F.count("*").alias("rows"), F.min("rank").alias("min_rank"), F.max("rank").alias("max_rank"))
            .orderBy(F.col("batch_id")))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
