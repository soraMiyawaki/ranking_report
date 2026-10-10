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

# 初回はYahoo!から。両APIを実行するときは ["yahoo", "rakuten"] に変更する。
SOURCES = ["yahoo", "rakuten"]
CREDENTIAL_MODE = "interactive"  # 手動初回用。定期実行時は "keyvault"
KEY_VAULT_URL = ""
SECRET_NAMES = {
    "yahoo_client_id": "yahoo-client-id",
    "rakuten_application_id": "rakuten-application-id",
    "rakuten_access_key": "rakuten-access-key",
}

# 通常は空欄。毎回新しい観測IDを発行する。
# 同じ取得を再実行するときだけ、以前のbatch_idを指定する。
# SUCCESSはスキップし、FAILED/RUNNINGの再利用は拒否する（新しい観測として取り直す）。
COLLECTION_BATCH_ID = ""

TABLES = {"yahoo": "bronze_yahoo_api_response", "rakuten": "bronze_rakuten_api_response"}
AUDIT_TABLE = "ops_collection_run"
if not SOURCES or len(SOURCES) != len(set(SOURCES)) or any(s not in TABLES for s in SOURCES):
    raise ValueError("Invalid SOURCES")
if CREDENTIAL_MODE not in {"interactive", "keyvault"}:
    raise ValueError("Invalid CREDENTIAL_MODE")
spark.conf.set("spark.sql.session.timeZone", "UTC")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable
from pyspark.sql import functions as F

RESPONSE_SCHEMA = "batch_id STRING,     run_id STRING,     source STRING,     request_scope STRING,     request_page INT,     observed_at TIMESTAMP,     http_status INT,     source_updated_at_raw STRING,     source_period_start_raw STRING,     source_period_end_raw STRING,     record_count BIGINT,     response_body STRING"
RUN_SCHEMA = "batch_id STRING,     run_id STRING,     source STRING,     request_scope STRING,     started_at TIMESTAMP,     finished_at TIMESTAMP,     status STRING,     last_http_status INT,     received_pages INT,     record_count BIGINT,     error_code STRING,     bronze_stored BOOLEAN"

# 取得用NotebookではCREATE TABLEも、自動的な列追加も行わない。
def check_table(table, schema):
    if not spark.catalog.tableExists(table):
        raise RuntimeError(f"{table}が未作成です。00_create_bronze_tablesを先に実行してください。")
    expected = spark.createDataFrame([], schema).schema
    actual = spark.table(table).schema
    signature = lambda s: [(f.name, f.dataType.simpleString()) for f in s.fields]
    if signature(expected) != signature(actual):
        raise RuntimeError(f"{table}の列定義が異なります。テーブル作成用Notebookで確認してください。")
    DeltaTable.forName(spark, table)  # 対象が既存Deltaテーブルであることも確認

for source in SOURCES:
    check_table(TABLES[source], RESPONSE_SCHEMA)
check_table(AUDIT_TABLE, RUN_SCHEMA)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

required_names = []
if "yahoo" in SOURCES:
    required_names.append("yahoo_client_id")
if "rakuten" in SOURCES:
    required_names.extend(["rakuten_application_id", "rakuten_access_key"])

if CREDENTIAL_MODE == "interactive":
    import ipywidgets as widgets
    credential_inputs = {}
    for name in required_names:
        widget = widgets.Password(description=name, style={"description_width": "initial"})
        credential_inputs[name] = widget
        display(widget)
    print("必要な認証情報を入力し、次のセルへ進んでください。")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
import math
import time
from datetime import datetime, timezone
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError


class IngestError(Exception):
    def __init__(self, code, http_status=None, body=None):
        super().__init__(code)
        self.code = code
        self.http_status = http_status
        self.body = body  # エラー応答の本文（原因調査用にBronzeへ残す）


def utc_now():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def request_spec(source, credentials, page=1):
    if source == "yahoo":
        if not credentials.get("yahoo_client_id"):
            raise IngestError("missing_yahoo_client_id")
        endpoint = "https://shopping.yahooapis.jp/ShoppingWebService/V2/queryRanking"
        params = {"appid": credentials["yahoo_client_id"], "type": "ranking", "hits": 100, "offset": 0}
        headers = {"Accept": "application/json"}
        scope = "type=ranking;category=overall;hits=100;offset=0"
    elif source == "rakuten":
        if not credentials.get("rakuten_application_id") or not credentials.get("rakuten_access_key"):
            raise IngestError("missing_rakuten_credentials")
        endpoint = "https://openapi.rakuten.co.jp/ichibaranking/api/IchibaItem/Ranking/20220601"
        params = {"applicationId": credentials["rakuten_application_id"], "format": "json",
                  "formatVersion": 2, "page": page}
        headers = {"Accept": "application/json", "accessKey": credentials["rakuten_access_key"]}
        scope = "genre=overall;period=default;carrier=default;formatVersion=2;requested_top=100"
    else:
        raise IngestError("unknown_source")
    return endpoint, params, headers, scope


def fetch(endpoint, params, headers, opener=urlopen, sleeper=time.sleep):
    for attempt in range(3):
        try:
            req = Request(endpoint + "?" + urlencode(params), headers=headers)
            with opener(req, timeout=30) as response:
                body = response.read().decode("utf-8-sig")
                return body, response.status
        except HTTPError as exc:
            status = exc.code
            retry_after = exc.headers.get("Retry-After") if exc.headers else None
            try:
                error_body = exc.read().decode("utf-8-sig", errors="replace")
            except Exception:
                error_body = None
            exc.close()
            if status not in {429, 500, 502, 503, 504} or attempt == 2:
                raise IngestError(f"http_{status}", status, error_body) from None
            delay = float(2 ** attempt)
            if retry_after:
                try:
                    delay = float(retry_after)
                except ValueError:
                    raise IngestError("retry_after_date_requires_later_execution", status) from None
                if not math.isfinite(delay) or delay < 0 or delay > 30:
                    raise IngestError("retry_after_requires_later_execution", status) from None
            sleeper(delay)
        except (URLError, TimeoutError, OSError):
            if attempt == 2:
                raise IngestError("connection_failed") from None
            sleeper(2 ** attempt)
    raise IngestError("request_failed")


def describe(source, payload):
    if not isinstance(payload, dict):
        raise IngestError("response_not_object")
    if source == "yahoo":
        root = payload.get("keyword_ranking")
        if not isinstance(root, dict):
            raise IngestError("unexpected_yahoo_root")
        rows = root.get("ranking_data")
        meta = root.get("meta")
        if not isinstance(meta, dict):
            raise IngestError("missing_yahoo_meta")
        dates = (meta.get("last_modified"), meta.get("start_date"), meta.get("end_date"))
    elif source == "rakuten":
        rows = payload.get("Items", payload.get("items"))
        dates = (payload.get("lastBuildDate"), None, None)
    else:
        raise IngestError("unknown_source")
    if not isinstance(rows, list) or not all(isinstance(r, dict) for r in rows):
        raise IngestError("unexpected_ranking_array")
    if any(value is not None and not isinstance(value, str) for value in dates):
        raise IngestError("unexpected_source_date_type")
    return len(rows), dates


def collect(source, credentials, run_id, batch_id, opener=urlopen, sleeper=time.sleep):
    """戻り値：Bronze行、実行結果。途中取得も残し、完全な実行と区別する。"""
    started = utc_now()
    pages = [1] if source == "yahoo" else [1, 2, 3, 4]
    response_rows, expected_updates = [], set()
    last_status, code, status = None, None, "SUCCESS"
    scope = ""
    try:
        for page in pages:
            endpoint, params, headers, scope = request_spec(source, credentials, page)
            body, last_status = fetch(endpoint, params, headers, opener, sleeper)
            observed = utc_now()
            # 解析に失敗した応答も、応答を受け取れたことをBronzeへ残す。
            row = {"batch_id": batch_id, "run_id": run_id, "source": source,
                   "request_scope": scope, "request_page": page, "observed_at": observed,
                   "http_status": last_status, "source_updated_at_raw": None,
                   "source_period_start_raw": None, "source_period_end_raw": None,
                   "record_count": None, "response_body": body}
            response_rows.append(row)
            try:
                payload = json.loads(body)
            except ValueError:
                raise IngestError("invalid_response_json", last_status) from None
            count, dates = describe(source, payload)
            row.update(record_count=count, source_updated_at_raw=dates[0],
                       source_period_start_raw=dates[1], source_period_end_raw=dates[2])
            if dates[0] is not None:
                expected_updates.add(dates[0])
            if len(expected_updates) > 1:
                raise IngestError("source_changed_during_pagination", last_status)
            if count == 0:
                status = "EMPTY" if page == 1 else "SUCCESS"
                break
            if source == "rakuten" and page < 4:
                sleeper(2)  # 暫定値。公式利用条件・実測で調整する。
    except IngestError as exc:
        status, code = "FAILED", exc.code
        if exc.http_status is not None:
            last_status = exc.http_status
        if exc.body is not None:
            # HTTPエラーの応答本文も原文として残す。認証情報が含まれていれば伏せる。
            body = exc.body
            for secret in credentials.values():
                if secret:
                    body = body.replace(secret, "***")
            response_rows.append({"batch_id": batch_id, "run_id": run_id, "source": source,
                                  "request_scope": scope, "request_page": page,
                                  "observed_at": utc_now(), "http_status": exc.http_status,
                                  "source_updated_at_raw": None, "source_period_start_raw": None,
                                  "source_period_end_raw": None, "record_count": None,
                                  "response_body": body})
    finished = utc_now()
    audit = {"batch_id": batch_id, "run_id": run_id, "source": source,
             "request_scope": scope, "started_at": started, "finished_at": finished,
             "status": status, "last_http_status": last_status,
             "received_pages": len(response_rows),
             "record_count": sum(r["record_count"] or 0 for r in response_rows),
             "error_code": code, "bronze_stored": False}
    return response_rows, audit


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 入力フォームまたはKey Vaultから認証情報を読み取り、フォームの値を消去する。
if CREDENTIAL_MODE == "interactive":
    credentials = {name: credential_inputs[name].value.strip() for name in required_names}
    for widget in credential_inputs.values():
        widget.value = ""
        widget.close()
else:
    if not KEY_VAULT_URL:
        raise ValueError("KEY_VAULT_URLが未設定です。")
    credentials = {name: notebookutils.credentials.getSecret(KEY_VAULT_URL, SECRET_NAMES[name])
                   for name in required_names}
missing = [name for name in required_names if not credentials.get(name)]
if missing:
    raise ValueError(f"認証情報が未入力です: {missing}")
print("認証情報を読み取りました。")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import uuid

RESPONSE_KEYS = ["run_id", "source", "request_page"]
RUN_KEYS = ["batch_id", "run_id", "source"]


def merge_rows(table, rows, schema, keys):
    if not rows:
        return
    struct = spark.createDataFrame([], schema).schema
    df = spark.createDataFrame([tuple(r[f] for f in struct.fieldNames()) for r in rows], struct)
    condition = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    (DeltaTable.forName(spark, table).alias("t")
        .merge(df.alias("s"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())


if "credentials" not in globals():
    raise RuntimeError("認証情報が未読み取りです。入力フォームのセルから順に実行し直してください。")
batch_id = COLLECTION_BATCH_ID or uuid.uuid4().hex
results = {}
for source in SOURCES:
    previous = {r["status"]: r["bronze_stored"] for r in spark.table(AUDIT_TABLE)
                .where((F.col("batch_id") == batch_id) & (F.col("source") == source))
                .select("status", "bronze_stored").collect()}
    if previous.get("SUCCESS"):
        print(f"{source}: batch_id={batch_id} は取得済みのためスキップします。")
        results[source] = "SKIPPED"
        continue
    if previous:
        raise RuntimeError(f"{source}: batch_id={batch_id} に完了していない実行があります"
                           f"（{sorted(previous)}）。COLLECTION_BATCH_IDを空欄に戻して取り直してください。")

    run_id = uuid.uuid4().hex
    started = utc_now()
    merge_rows(AUDIT_TABLE, [{"batch_id": batch_id, "run_id": run_id, "source": source,
                              "request_scope": None, "started_at": started, "finished_at": None,
                              "status": "RUNNING", "last_http_status": None, "received_pages": 0,
                              "record_count": 0, "error_code": None, "bronze_stored": False}],
               RUN_SCHEMA, RUN_KEYS)
    response_rows, audit = collect(source, credentials, run_id, batch_id)
    audit["started_at"] = started
    merge_rows(TABLES[source], response_rows, RESPONSE_SCHEMA, RESPONSE_KEYS)
    audit["bronze_stored"] = len(response_rows) > 0
    merge_rows(AUDIT_TABLE, [audit], RUN_SCHEMA, RUN_KEYS)
    results[source] = audit["status"]
    print(f"{source}: status={audit['status']} pages={audit['received_pages']} "
          f"records={audit['record_count']} error={audit['error_code']}")

del credentials  # 認証情報は実行ごとに読み直す
LAST_BATCH_ID = batch_id  # 後続の確認セル用。COLLECTION_BATCH_IDは書き換えない
display(spark.table(AUDIT_TABLE).where(F.col("batch_id") == batch_id).orderBy("source"))
failed = [s for s, status in results.items() if status not in {"SUCCESS", "SKIPPED"}]
if failed:
    raise RuntimeError(f"正常に完了していない取得元があります: {failed}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 実際に保存したYahoo!データをSpark SQLで確認する。テーブルは作成しない。
if "yahoo" in SOURCES:
    (spark.table(TABLES["yahoo"])
        .where(F.col("batch_id") == LAST_BATCH_ID)
        .createOrReplaceTempView("current_yahoo_response"))
    display(spark.sql("""
        WITH parsed AS (
            SELECT from_json(
                response_body,
                'STRUCT<keyword_ranking:STRUCT<ranking_data:ARRAY<STRUCT<rank:INT,query:STRING,pre_rank:INT,vector:STRING>>>>'
            ) AS data
            FROM current_yahoo_response
        ), expanded AS (
            SELECT explode(data.keyword_ranking.ranking_data) AS item FROM parsed
        )
        SELECT item.rank AS rank, item.query AS keyword,
               item.pre_rank AS previous_rank, item.vector AS direction
        FROM expanded ORDER BY rank
    """))

if "rakuten" in SOURCES:
    display(spark.table(TABLES["rakuten"])
        .where(F.col("batch_id") == LAST_BATCH_ID)
        .select("run_id", "request_page", "record_count", "source_updated_at_raw", "observed_at")
        .orderBy("request_page"))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from urllib.request import urlopen

with urlopen("https://api.ipify.org", timeout=10) as response:
    print(response.read().decode().strip())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
