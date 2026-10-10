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
spark.conf.set("spark.sql.session.timeZone", "UTC")

for table in ["lh_ec_silver.dbo.silver_yahoo_keyword_rank", "lh_ec_silver.dbo.silver_rakuten_item_rank"]:
    if not spark.catalog.tableExists(table):
        raise RuntimeError(f"{table}が未作成です。create_silverを先に実行してください。")


def check(name, sql):
    """検査用SQLが1行でも返したら、内容を示して停止する。"""
    rows = spark.sql(sql).limit(5).collect()
    if rows:
        raise RuntimeError(f"{name}: {[r.asDict() for r in rows]}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Yahoo：未処理runの検索語ランキングを1行1順位に展開する。
# 数値は文字列で読んでから型変換する。日付はタイムゾーン表記がないため、日本の暦日としてDATEで保持する。
spark.sql("""
CREATE OR REPLACE TEMP VIEW yahoo_new AS
WITH done AS (   -- 変換済みのrun（解釈済み日付が空のrunは再処理して埋める）
    SELECT DISTINCT run_id FROM lh_ec_silver.dbo.silver_yahoo_keyword_rank
    WHERE source_updated_date IS NOT NULL OR source_updated_at_raw IS NULL
), runs AS (
    SELECT run_id FROM lh_ec_bronze.dbo.ops_collection_run
    WHERE source = 'yahoo' AND status = 'SUCCESS' AND bronze_stored
      AND run_id NOT IN (SELECT run_id FROM done)
), parsed AS (
    SELECT b.batch_id, b.run_id, b.observed_at,
           b.source_updated_at_raw, b.source_period_start_raw, b.source_period_end_raw,
           from_json(b.response_body,
             'STRUCT<keyword_ranking:STRUCT<ranking_data:ARRAY<STRUCT<rank:STRING,query:STRING,pre_rank:STRING,vector:STRING>>>>'
           ) AS data
    FROM lh_ec_bronze.dbo.bronze_yahoo_api_response b
    JOIN runs r ON b.run_id = r.run_id
    WHERE b.http_status = 200
)
SELECT batch_id, run_id, observed_at,
       source_updated_at_raw, source_period_start_raw, source_period_end_raw,
       CAST(item.rank AS INT)     AS rank,
       item.query                 AS keyword,
       CAST(item.pre_rank AS INT) AS previous_rank,
       item.vector                AS direction,
       current_timestamp()        AS processed_at,
       to_date(source_updated_at_raw, 'yyyy-MM-dd')   AS source_updated_date,
       to_date(source_period_start_raw, 'yyyy-MM-dd') AS source_period_start_date,
       to_date(source_period_end_raw, 'yyyy-MM-dd')   AS source_period_end_date
FROM parsed
LATERAL VIEW explode(data.keyword_ranking.ranking_data) t AS item
""")
spark.sql("CACHE TABLE yahoo_new")  # 検査とMERGEで同じ内容を使うため固定する

check("Yahoo：順位が空の行がある", "SELECT run_id FROM yahoo_new WHERE rank IS NULL")
check("Yahoo：同じrunで順位が重複", "SELECT run_id, rank, count(*) AS n FROM yahoo_new GROUP BY run_id, rank HAVING n > 1")
check("Yahoo：日付を解釈できない", """
    SELECT DISTINCT source_updated_at_raw, source_period_start_raw, source_period_end_raw FROM yahoo_new
    WHERE (source_updated_at_raw IS NOT NULL AND source_updated_date IS NULL)
       OR (source_period_start_raw IS NOT NULL AND source_period_start_date IS NULL)
       OR (source_period_end_raw IS NOT NULL AND source_period_end_date IS NULL)
""")

# runごとの件数と欠けている順位（欠けていても失敗にはしない）
display(spark.sql("""
    SELECT run_id, count(*) AS rows,
           array_except(sequence(1, 100), collect_set(rank)) AS missing_ranks
    FROM yahoo_new GROUP BY run_id
""").toPandas())

spark.sql("""
MERGE INTO lh_ec_silver.dbo.silver_yahoo_keyword_rank t
USING yahoo_new s
ON t.run_id = s.run_id AND t.rank = s.rank
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")
spark.sql("UNCACHE TABLE yahoo_new")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 楽天：未処理runの商品ランキングを1行1順位に展開し、100位までに絞る。
# lastBuildDateはRFC 1123形式（例 "Fri, 09 Oct 2026 00:00:00 +0900"）。曜日を除いて解釈し、UTCで保持する。
spark.sql("""
CREATE OR REPLACE TEMP VIEW rakuten_new AS
WITH done AS (
    SELECT DISTINCT run_id FROM lh_ec_silver.dbo.silver_rakuten_item_rank
    WHERE source_updated_at IS NOT NULL OR source_updated_at_raw IS NULL
), runs AS (
    SELECT run_id FROM lh_ec_bronze.dbo.ops_collection_run
    WHERE source = 'rakuten' AND status = 'SUCCESS' AND bronze_stored
      AND run_id NOT IN (SELECT run_id FROM done)
), parsed AS (
    SELECT b.batch_id, b.run_id, b.observed_at, b.source_updated_at_raw,
           from_json(b.response_body,
             'STRUCT<Items:ARRAY<STRUCT<rank:STRING,itemCode:STRING,itemName:STRING,itemPrice:STRING,
                     shopCode:STRING,shopName:STRING,genreId:STRING,reviewCount:STRING,reviewAverage:STRING,
                     pointRate:STRING,postageFlag:STRING,availability:STRING,itemUrl:STRING>>>'
           ) AS data
    FROM lh_ec_bronze.dbo.bronze_rakuten_api_response b
    JOIN runs r ON b.run_id = r.run_id
    WHERE b.http_status = 200
), items AS (
    SELECT batch_id, run_id, observed_at, source_updated_at_raw,
           CAST(item.rank AS INT)            AS rank,
           item.itemCode                     AS item_code,
           item.itemName                     AS item_name,
           CAST(item.itemPrice AS BIGINT)    AS item_price,
           item.shopCode                     AS shop_code,
           item.shopName                     AS shop_name,
           item.genreId                      AS genre_id,
           CAST(item.reviewCount AS BIGINT)  AS review_count,
           CAST(item.reviewAverage AS DOUBLE) AS review_average,
           CAST(item.pointRate AS INT)       AS point_rate,
           CAST(item.postageFlag AS INT)     AS postage_flag,
           CAST(item.availability AS INT)    AS availability,
           item.itemUrl                      AS item_url
    FROM parsed
    LATERAL VIEW explode(data.Items) t AS item
)
SELECT *,
       current_timestamp() AS processed_at,
       to_timestamp(regexp_replace(source_updated_at_raw, '^[A-Za-z]{3}, ', ''), 'dd MMM yyyy HH:mm:ss Z')
           AS source_updated_at
FROM items
WHERE rank IS NULL OR rank <= 100
""")
spark.sql("CACHE TABLE rakuten_new")

check("楽天：順位が空の行がある", "SELECT run_id FROM rakuten_new WHERE rank IS NULL")
check("楽天：同じrunで順位が重複", "SELECT run_id, rank, count(*) AS n FROM rakuten_new GROUP BY run_id, rank HAVING n > 1")
check("楽天：lastBuildDateを解釈できない", """
    SELECT DISTINCT source_updated_at_raw FROM rakuten_new
    WHERE source_updated_at_raw IS NOT NULL AND source_updated_at IS NULL
""")

display(spark.sql("""
    SELECT run_id, count(*) AS rows,
           array_except(sequence(1, 100), collect_set(rank)) AS missing_ranks
    FROM rakuten_new GROUP BY run_id
""").toPandas())

spark.sql("""
MERGE INTO lh_ec_silver.dbo.silver_rakuten_item_rank t
USING rakuten_new s
ON t.run_id = s.run_id AND t.rank = s.rank
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")
spark.sql("UNCACHE TABLE rakuten_new")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 処理結果の確認：Silverのrunごとの件数。
display(spark.sql("""
    SELECT 'yahoo' AS source, batch_id, run_id, source_updated_at_raw,
           CAST(source_updated_date AS STRING) AS source_updated,
           count(*) AS rows, min(rank) AS min_rank, max(rank) AS max_rank
    FROM lh_ec_silver.dbo.silver_yahoo_keyword_rank
    GROUP BY batch_id, run_id, source_updated_at_raw, source_updated_date
    UNION ALL
    SELECT 'rakuten', batch_id, run_id, source_updated_at_raw,
           CAST(source_updated_at AS STRING),
           count(*), min(rank), max(rank)
    FROM lh_ec_silver.dbo.silver_rakuten_item_rank
    GROUP BY batch_id, run_id, source_updated_at_raw, source_updated_at
    ORDER BY source, batch_id
""").toPandas())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
