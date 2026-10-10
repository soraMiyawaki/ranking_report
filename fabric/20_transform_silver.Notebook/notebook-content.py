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

# Bronzeの成功済み取得（SUCCESS かつ bronze_stored）から、Silverにまだ無いランキングの版だけを追加する。
# 版＝提供元の更新日時（Yahoo：更新日、楽天：lastBuildDate）。同じ版を何度取得しても、Silverには1回だけ入る。
# 同じ版を複数回取得している場合は、最後に観測したrunを採用する。テーブルは作成しない。
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

# Yahoo：新しい版の検索語ランキングを1行1順位に展開する。
# 数値は文字列で読んでから型変換する。日付はタイムゾーン表記がないため、日本の暦日としてDATEで保持する。
yahoo_new = spark.sql("""
WITH parsed AS (
    SELECT b.batch_id, b.run_id, b.observed_at,
           b.source_updated_at_raw, b.source_period_start_raw, b.source_period_end_raw,
           to_date(b.source_updated_at_raw, 'yyyy-MM-dd')   AS source_updated_date,
           to_date(b.source_period_start_raw, 'yyyy-MM-dd') AS source_period_start_date,
           to_date(b.source_period_end_raw, 'yyyy-MM-dd')   AS source_period_end_date,
           from_json(b.response_body,
             'STRUCT<keyword_ranking:STRUCT<ranking_data:ARRAY<STRUCT<rank:STRING,query:STRING,pre_rank:STRING,vector:STRING>>>>'
           ) AS data
    FROM lh_ec_bronze.dbo.bronze_yahoo_api_response b
    JOIN lh_ec_bronze.dbo.ops_collection_run r
      ON b.run_id = r.run_id AND r.source = 'yahoo' AND r.status = 'SUCCESS' AND r.bronze_stored
    WHERE b.http_status = 200
), latest_run AS (   -- 版ごとに最後に観測したrun
    SELECT run_id FROM (
        SELECT run_id, row_number() OVER (
                   PARTITION BY source_updated_date ORDER BY observed_at DESC, run_id DESC) AS rn
        FROM parsed
    ) WHERE rn = 1
), new_edition AS (  -- Silverにまだ無い版だけ（版が空の行は検査で止めるため残す）
    SELECT p.* FROM parsed p
    JOIN latest_run l ON p.run_id = l.run_id
    LEFT ANTI JOIN (SELECT DISTINCT source_updated_date FROM lh_ec_silver.dbo.silver_yahoo_keyword_rank) s
      ON p.source_updated_date = s.source_updated_date
)
SELECT batch_id, run_id, observed_at,
       source_updated_at_raw, source_period_start_raw, source_period_end_raw,
       CAST(item.rank AS INT)     AS rank,
       item.query                 AS keyword,
       CAST(item.pre_rank AS INT) AS previous_rank,
       item.vector                AS direction,
       current_timestamp()        AS processed_at,
       source_updated_date, source_period_start_date, source_period_end_date
FROM new_edition
LATERAL VIEW explode(data.keyword_ranking.ranking_data) t AS item
""")
# SQLで作る一時ビューは参照のたびに名前を解決し直し、Fabricの3段名（lakehouse.schema.table）を解決できない。
# そのため結果をキャッシュし、解決済みのDataFrameとして一時ビューに登録する。
yahoo_new.cache()  # 検査とMERGEで同じ内容を使うため固定する
yahoo_new.createOrReplaceTempView("yahoo_new")

check("Yahoo：更新日が空、または解釈できない", """
    SELECT DISTINCT run_id, source_updated_at_raw FROM yahoo_new WHERE source_updated_date IS NULL
""")
check("Yahoo：集計期間の日付を解釈できない", """
    SELECT DISTINCT source_period_start_raw, source_period_end_raw FROM yahoo_new
    WHERE (source_period_start_raw IS NOT NULL AND source_period_start_date IS NULL)
       OR (source_period_end_raw IS NOT NULL AND source_period_end_date IS NULL)
""")
check("Yahoo：順位が空の行がある", "SELECT run_id FROM yahoo_new WHERE rank IS NULL")
check("Yahoo：同じ版で順位が重複", """
    SELECT source_updated_date, rank, count(*) AS n FROM yahoo_new
    GROUP BY source_updated_date, rank HAVING n > 1
""")

# 追加する版ごとの件数と欠けている順位（欠けていても失敗にはしない）
display(spark.sql("""
    SELECT source_updated_date, run_id, count(*) AS rows,
           array_except(sequence(1, 100), collect_set(rank)) AS missing_ranks
    FROM yahoo_new GROUP BY source_updated_date, run_id ORDER BY source_updated_date
""").toPandas())

spark.sql("""
MERGE INTO lh_ec_silver.dbo.silver_yahoo_keyword_rank t
USING yahoo_new s
ON t.source_updated_date = s.source_updated_date AND t.rank = s.rank
WHEN NOT MATCHED THEN INSERT *
""")
yahoo_new.unpersist()
None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 楽天：新しい版の商品ランキングを1行1順位に展開し、100位までに絞る。
# lastBuildDateはRFC 1123形式（例 "Fri, 09 Oct 2026 00:00:00 +0900"）。曜日を除いて解釈し、UTCで保持する。
rakuten_new = spark.sql("""
WITH parsed AS (
    SELECT b.batch_id, b.run_id, b.observed_at, b.source_updated_at_raw,
           to_timestamp(regexp_replace(b.source_updated_at_raw, '^[A-Za-z]{3}, ', ''), 'dd MMM yyyy HH:mm:ss Z')
               AS source_updated_at,
           from_json(b.response_body,
             'STRUCT<Items:ARRAY<STRUCT<rank:STRING,itemCode:STRING,itemName:STRING,itemPrice:STRING,
                     shopCode:STRING,shopName:STRING,genreId:STRING,reviewCount:STRING,reviewAverage:STRING,
                     pointRate:STRING,postageFlag:STRING,availability:STRING,itemUrl:STRING>>>'
           ) AS data
    FROM lh_ec_bronze.dbo.bronze_rakuten_api_response b
    JOIN lh_ec_bronze.dbo.ops_collection_run r
      ON b.run_id = r.run_id AND r.source = 'rakuten' AND r.status = 'SUCCESS' AND r.bronze_stored
    WHERE b.http_status = 200
), latest_run AS (   -- 版ごとに最後に観測したrun（1回の取得は複数ページなので、runの最終観測時刻で比べる）
    SELECT run_id FROM (
        SELECT run_id, row_number() OVER (
                   PARTITION BY source_updated_at ORDER BY last_observed_at DESC, run_id DESC) AS rn
        FROM (SELECT run_id, source_updated_at, max(observed_at) AS last_observed_at
              FROM parsed GROUP BY run_id, source_updated_at)
    ) WHERE rn = 1
), new_edition AS (
    SELECT p.* FROM parsed p
    JOIN latest_run l ON p.run_id = l.run_id
    LEFT ANTI JOIN (SELECT DISTINCT source_updated_at FROM lh_ec_silver.dbo.silver_rakuten_item_rank) s
      ON p.source_updated_at = s.source_updated_at
)
SELECT batch_id, run_id, observed_at, source_updated_at_raw,
       CAST(item.rank AS INT)             AS rank,
       item.itemCode                      AS item_code,
       item.itemName                      AS item_name,
       CAST(item.itemPrice AS BIGINT)     AS item_price,
       item.shopCode                      AS shop_code,
       item.shopName                      AS shop_name,
       item.genreId                       AS genre_id,
       CAST(item.reviewCount AS BIGINT)   AS review_count,
       CAST(item.reviewAverage AS DOUBLE) AS review_average,
       CAST(item.pointRate AS INT)        AS point_rate,
       CAST(item.postageFlag AS INT)      AS postage_flag,
       CAST(item.availability AS INT)     AS availability,
       item.itemUrl                       AS item_url,
       current_timestamp()                AS processed_at,
       source_updated_at
FROM new_edition
LATERAL VIEW explode(data.Items) t AS item
WHERE CAST(item.rank AS INT) IS NULL OR CAST(item.rank AS INT) <= 100
""")
rakuten_new.cache()
rakuten_new.createOrReplaceTempView("rakuten_new")

check("楽天：lastBuildDateが空、または解釈できない", """
    SELECT DISTINCT run_id, source_updated_at_raw FROM rakuten_new WHERE source_updated_at IS NULL
""")
check("楽天：順位が空の行がある", "SELECT run_id FROM rakuten_new WHERE rank IS NULL")
check("楽天：同じ版で順位が重複", """
    SELECT source_updated_at, rank, count(*) AS n FROM rakuten_new
    GROUP BY source_updated_at, rank HAVING n > 1
""")

display(spark.sql("""
    SELECT source_updated_at, run_id, count(*) AS rows,
           array_except(sequence(1, 100), collect_set(rank)) AS missing_ranks
    FROM rakuten_new GROUP BY source_updated_at, run_id ORDER BY source_updated_at
""").toPandas())

spark.sql("""
MERGE INTO lh_ec_silver.dbo.silver_rakuten_item_rank t
USING rakuten_new s
ON t.source_updated_at = s.source_updated_at AND t.rank = s.rank
WHEN NOT MATCHED THEN INSERT *
""")
rakuten_new.unpersist()
None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 処理結果の確認：Silverの版ごとの件数。
display(spark.sql("""
    SELECT 'yahoo' AS source, CAST(source_updated_date AS STRING) AS edition, source_updated_at_raw,
           count(DISTINCT run_id) AS runs, count(*) AS rows, min(rank) AS min_rank, max(rank) AS max_rank
    FROM lh_ec_silver.dbo.silver_yahoo_keyword_rank
    GROUP BY source_updated_date, source_updated_at_raw
    UNION ALL
    SELECT 'rakuten', CAST(source_updated_at AS STRING), source_updated_at_raw,
           count(DISTINCT run_id), count(*), min(rank), max(rank)
    FROM lh_ec_silver.dbo.silver_rakuten_item_rank
    GROUP BY source_updated_at, source_updated_at_raw
    ORDER BY source, edition
""").toPandas())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
