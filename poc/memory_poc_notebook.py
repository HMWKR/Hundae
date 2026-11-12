# Databricks notebook source
# MAGIC %md
# MAGIC # 들를까 — 사용자별 기억 개념 검증 (PoC)
# MAGIC
# MAGIC 가상의 예시 데이터로 아래 여섯 가지를 확인합니다. **실제 사용자·위치 데이터는 올리지 않습니다.**
# MAGIC
# MAGIC | 순서 | 확인하는 것 | 성공 기준 |
# MAGIC |---|---|---|
# MAGIC | 1 | 가상 사용자 5명의 사건 기록을 브론즈에 적재 | 적재 건수 일치 |
# MAGIC | 2 | 실버 정제, 골드 기억 문장 병합(CDF 켬) | 방문 1건이 기억 문장 1건이 됨 |
# MAGIC | 3 | 다음 날 치 30건 + 정정 1건을 넣고 변경분 조회 | 추가 10행, 수정 1행만 나옴 |
# MAGIC | 4 | 벡터 인덱스 트리거 동기화 | 새 기억이 검색됨 |
# MAGIC | 5 | "저번에 갔던 곳"(날짜순), "바다가 보이는 카페"(뜻 검색) | 다른 사용자의 기록이 섞이지 않음 |
# MAGIC | 6 | 사용자 1명 삭제 | 테이블과 인덱스에서 모두 사라짐 |
# MAGIC
# MAGIC **실행 방법**: 위에서부터 차례로 실행합니다. 벡터 검색을 쓸 수 없는 환경이면 위젯 `use_vector_search`를 `no`로 두면 1~3번, 5번의 날짜순 조회, 6번의 테이블 삭제까지만 확인합니다.
# MAGIC
# MAGIC **다시 실행할 때**: 1번 셀이 브론즈를 1일 차 상태로 덮어쓰고, 실버·골드는 병합으로 다시 맞춰집니다.

# COMMAND ----------

# MAGIC %pip install --quiet databricks-vectorsearch

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. 설정

# COMMAND ----------

import re
import time

dbutils.widgets.text("catalog", "workspace", "카탈로그")
dbutils.widgets.text("schema", "memory_poc", "스키마")
dbutils.widgets.dropdown("use_vector_search", "yes", ["yes", "no"], "벡터 검색 단계 실행")
dbutils.widgets.text("vs_endpoint", "memory_poc_vs", "벡터 검색 엔드포인트")
dbutils.widgets.text("embedding_endpoint", "", "임베딩 모델 엔드포인트(비우면 자동 선택)")

CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA = dbutils.widgets.get("schema").strip()
for name in (CATALOG, SCHEMA):
    assert re.fullmatch(r"[A-Za-z0-9_]+", name), f"영문·숫자·밑줄만 쓸 수 있습니다: {name}"
USE_VS = dbutils.widgets.get("use_vector_search") == "yes"
VS_ENDPOINT = dbutils.widgets.get("vs_endpoint").strip()

NS = f"{CATALOG}.{SCHEMA}"
BRONZE = f"{NS}.events_raw"
PLACES_T = f"{NS}.places_dim"
SILVER = f"{NS}.visits_silver"
GOLD = f"{NS}.memory_facts"
INDEX = f"{NS}.memory_facts_index"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {NS}")

results = []


def check(name, ok, detail=""):
    """확인 결과를 모아 두고 바로 출력한다."""
    results.append((name, "PASS" if ok else "FAIL", str(detail)))
    print("PASS" if ok else "FAIL", "|", name, "|", detail)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 가상 데이터 생성기
# MAGIC
# MAGIC 장소 이름은 전부 지어낸 것이고, 사용자 키는 무작위 값입니다. 시드가 고정되어 있어 실행할 때마다 같은 데이터가 나옵니다.

# COMMAND ----------

import random
import uuid
from datetime import datetime, timedelta

SEED = 42
BASE_DAY = datetime(2026, 11, 1, 9, 0, 0)  # 가상의 기준 시각. 1일 차 기록은 모두 이 시각 이전이다.

# (장소 ID, 이름, 유형, 세부 유형, 지역, 설명)
PLACES = [
    ("p01", "파도소리 카페", "먹을 곳", "카페", "강원 강릉", "바다가 보이는 2층 창가 자리가 있는 카페"),
    ("p02", "솔밭 베이커리", "먹을 곳", "빵집", "경기 양평", "갓 구운 소금빵으로 알려진 빵집"),
    ("p03", "장터 국밥", "먹을 곳", "한식", "전북 전주", "아침 일찍 여는 시장 골목 국밥집"),
    ("p04", "돌담 막국수", "먹을 곳", "면 요리", "강원 춘천", "메밀 향이 진한 막국수집"),
    ("p05", "포구 횟집", "먹을 곳", "해산물", "전남 여수", "항구 바로 앞에 있는 횟집"),
    ("p06", "언덕 위 찻집", "먹을 곳", "찻집", "경북 경주", "기와지붕이 내려다보이는 조용한 찻집"),
    ("p07", "해맞이 전망대", "볼 곳", "전망대", "강원 속초", "해안 절벽 위에서 노을을 볼 수 있는 전망대"),
    ("p08", "물안개 수목원", "볼 곳", "수목원", "경기 가평", "호수를 끼고 걷는 산책로가 있는 수목원"),
    ("p09", "옛성 둘레길", "볼 곳", "산책로", "경북 경주", "성벽을 따라 걷는 평탄한 둘레길"),
    ("p10", "등대 공원", "볼 곳", "공원", "부산 기장", "하얀 등대와 바다 산책로가 있는 공원"),
    ("p11", "대숲 정원", "볼 곳", "정원", "전남 담양", "대나무 숲 사이로 난 그늘진 정원"),
    ("p12", "별빛 천문대", "볼 곳", "천문대", "강원 영월", "밤하늘을 볼 수 있는 산꼭대기 천문대"),
]
PLACE_NAME = {p[0]: p[1] for p in PLACES}


def make_users(rng, n=5):
    """우리만 아는 내부 식별 ID. 실제 식별자를 변환한 값이 아니라 무작위 값이다."""
    return ["u_" + uuid.UUID(int=rng.getrandbits(128)).hex[:12] for _ in range(n)]


def make_suggestion(rng, user, when, outcome=None, place=None):
    """제안 1건이 남기는 사건 기록. 제안, 응답, (수락했다면) 방문의 2~3건이다."""
    place = place or rng.choice(PLACES)
    sid = uuid.UUID(int=rng.getrandbits(128)).hex[:16]
    detour = rng.randint(2, 14)
    outcome = outcome or rng.choices(["ACCEPTED", "DECLINED", "DEFERRED"], weights=[35, 45, 20])[0]
    base = {
        "user_key": user,
        "place_id": place[0],
        "detour_min": detour,
        "companions": rng.random() < 0.6,
        "trip_type": rng.choice(["나들이", "장거리", "퇴근길"]),
    }
    rows = [
        dict(base, event_id=f"{sid}-s", event_type="SUGGESTED", event_ts=when),
        dict(base, event_id=f"{sid}-o", event_type=outcome, event_ts=when + timedelta(seconds=8)),
    ]
    if outcome == "ACCEPTED":
        rows.append(dict(base, event_id=f"{sid}-v", event_type="VISITED", event_ts=when + timedelta(minutes=detour + 5)))
    return rows


def make_day1(rng, users, per_user=26, days=90):
    """1일 차 적재분. 지난 90일의 기록과, 중복 전송된 3건을 돌려준다."""
    rows = []
    for user in users:
        for _ in range(per_user):
            when = BASE_DAY - timedelta(days=rng.randint(1, days), minutes=rng.randint(0, 600))
            rows += make_suggestion(rng, user, when)
    dups = [dict(r) for r in rng.sample(rows, 3)]
    return rows, dups


def make_day2(rng, users, day1_rows):
    """2일 차 적재분. 수락·방문으로 끝난 제안 10건(사건 30건)과 정정 1건을 돌려준다."""
    rows = []
    for i in range(10):
        when = BASE_DAY + timedelta(days=1, minutes=30 * i)
        # 첫 건은 뜻 검색 시험을 위해 첫 사용자가 바다 전망 카페(p01)에 들른 것으로 고정한다.
        rows += make_suggestion(rng, users[i % len(users)], when, outcome="ACCEPTED", place=PLACES[0] if i == 0 else None)
    # 정정: 1일 차 방문 기록 하나의 동승 여부가 바뀌어 같은 event_id로 다시 들어온다.
    target = next(r for r in day1_rows if r["event_type"] == "VISITED")
    correction = dict(target, companions=not target["companions"])
    return rows, correction


# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. 브론즈 적재

# COMMAND ----------

EVENT_COLS = ["event_id", "user_key", "event_type", "event_ts", "place_id", "detour_min", "companions", "trip_type", "ingested_at"]
EVENT_SCHEMA = (
    "event_id STRING, user_key STRING, event_type STRING, event_ts TIMESTAMP, place_id STRING, "
    "detour_min INT, companions BOOLEAN, trip_type STRING, ingested_at TIMESTAMP"
)


def to_df(rows, ingested_at):
    data = [tuple(dict(r, ingested_at=ingested_at)[c] for c in EVENT_COLS) for r in rows]
    return spark.createDataFrame(data, EVENT_SCHEMA)


rng = random.Random(SEED)
USERS = make_users(rng)
day1_rows, day1_dups = make_day1(rng, USERS)
day2_rows, correction = make_day2(rng, USERS, day1_rows)
QUERY_USER = USERS[0]   # 질의 시험에 쓰는 사용자
DELETE_USER = USERS[4]  # 삭제 시험에 쓰는 사용자

places_schema = "place_id STRING, place_name STRING, place_type STRING, place_subtype STRING, region STRING, place_desc STRING"
spark.createDataFrame(PLACES, places_schema).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(PLACES_T)

# 브론즈는 들어온 그대로 쌓기만 하는 테이블이다. 다시 실행할 수 있도록 1일 차 상태로 덮어쓴다.
to_df(day1_rows + day1_dups, BASE_DAY + timedelta(hours=18)).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(BRONZE)

n_bronze = spark.table(BRONZE).count()
check("1. 브론즈 적재 건수", n_bronze == len(day1_rows) + len(day1_dups), f"{n_bronze}건 (중복 전송 {len(day1_dups)}건 포함)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. 실버 정제와 골드 기억 문장
# MAGIC
# MAGIC 골드 테이블에만 CDF를 켭니다. 브론즈는 추가만 하므로 CDF가 필요 없고, 변경분 추적이 필요한 곳은 병합으로 갱신되는 골드입니다.
# MAGIC
# MAGIC 병합의 `WHEN MATCHED AND ...` 조건이 중요합니다. 조건 없이 갱신하면 내용이 같아도 모든 행이 "수정됨"으로 기록되어, 변경분만 처리한다는 목적이 사라집니다.

# COMMAND ----------

# ▼ 직접 바꿔 볼 곳: 기억 문장에 무엇을 담을지 정하는 식이다.
#   담는 내용이 많을수록 답할 수 있는 질문이 늘지만, 그만큼 민감한 정보가 문장에 남는다.
#   (예: 날짜를 '월'까지만 남길지, 동승 여부를 넣을지, 이동 유형을 넣을지)
FACT_TEXT_SQL = """
concat(
  date_format(v.visited_on, 'yyyy-MM-dd'), ', ',
  CASE WHEN v.companions THEN '동승자와 함께 ' ELSE '혼자 ' END,
  p.region, '의 ', p.place_name, '(', p.place_type, ', ', p.place_subtype, ')에 들렀다. ',
  p.place_desc, '. 이동 유형: ', v.trip_type, '.'
)
"""

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER} (
  visit_id STRING NOT NULL, user_key STRING NOT NULL, place_id STRING, visited_on DATE, visited_at TIMESTAMP,
  companions BOOLEAN, trip_type STRING, detour_min INT
)""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD} (
  fact_id STRING NOT NULL, user_key STRING NOT NULL, place_id STRING, place_name STRING, place_type STRING,
  place_subtype STRING, region STRING, visited_on DATE, visited_at TIMESTAMP, companions BOOLEAN, trip_type STRING,
  fact_text STRING
)
TBLPROPERTIES (delta.enableChangeDataFeed = true)""")


def merge_silver():
    """브론즈의 방문 기록을 event_id로 중복 제거해 실버에 맞춘다. 나중에 들어온 기록이 이긴다."""
    spark.sql(f"""
    MERGE INTO {SILVER} t
    USING (
      SELECT event_id AS visit_id, user_key, place_id, CAST(event_ts AS DATE) AS visited_on, event_ts AS visited_at,
             companions, trip_type, detour_min
      FROM (
        SELECT *, row_number() OVER (PARTITION BY event_id ORDER BY ingested_at DESC) AS rn
        FROM {BRONZE}
        WHERE event_type = 'VISITED'
      )
      WHERE rn = 1
    ) s
    ON t.visit_id = s.visit_id
    WHEN MATCHED AND (t.companions <> s.companions OR t.place_id <> s.place_id OR t.visited_at <> s.visited_at)
      THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    WHEN NOT MATCHED BY SOURCE THEN DELETE""")


def merge_gold():
    """실버의 방문 1건을 기억 문장 1건으로 만든다. 문장이 실제로 바뀐 행만 갱신한다."""
    spark.sql(f"""
    MERGE INTO {GOLD} t
    USING (
      SELECT v.visit_id AS fact_id, v.user_key, v.place_id, p.place_name, p.place_type, p.place_subtype, p.region,
             v.visited_on, v.visited_at, v.companions, v.trip_type,
             {FACT_TEXT_SQL} AS fact_text
      FROM {SILVER} v
      JOIN {PLACES_T} p ON v.place_id = p.place_id
    ) s
    ON t.fact_id = s.fact_id
    WHEN MATCHED AND NOT (t.fact_text <=> s.fact_text) THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    WHEN NOT MATCHED BY SOURCE THEN DELETE""")


def nightly_batch():
    """야간 배치 한 번. 운영에서는 브론즈를 스트리밍 체크포인트(AvailableNow)로 증분 읽기 하는 것이 맞고,
    여기서는 규모가 작아 전체를 읽고 병합 조건으로 바뀐 것만 반영한다."""
    merge_silver()
    merge_gold()


def gold_version():
    return spark.sql(f"DESCRIBE HISTORY {GOLD} LIMIT 1").first()["version"]


def gold_changes(since_version):
    """since_version 이후에 골드에서 바뀐 행 수를 변경 종류별로 돌려준다."""
    if gold_version() <= since_version:
        return {}
    rows = spark.sql(
        f"SELECT _change_type, count(*) AS n FROM table_changes('{GOLD}', {since_version + 1}) GROUP BY _change_type"
    ).collect()
    return {r["_change_type"]: r["n"] for r in rows}


nightly_batch()

expected_visits_day1 = len({r["event_id"] for r in day1_rows if r["event_type"] == "VISITED"})
n_gold = spark.table(GOLD).count()
check("2. 기억 문장 수 = 방문 수(중복 제거 후)", n_gold == expected_visits_day1, f"{n_gold}건")

cdf_prop = spark.sql(f"SHOW TBLPROPERTIES {GOLD}").filter("key = 'delta.enableChangeDataFeed'").first()
check("2. 골드 테이블에 CDF가 켜져 있음", cdf_prop is not None and cdf_prop["value"] == "true")

display(spark.table(GOLD).select("user_key", "visited_on", "fact_text").orderBy("visited_at", ascending=False).limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 벡터 인덱스 만들기 (선택)
# MAGIC
# MAGIC 골드 테이블을 원본으로 하는 Delta Sync 인덱스를 **트리거 방식**으로 만듭니다. 엔드포인트와 인덱스를 처음 만들 때는 몇 분에서 십여 분이 걸립니다.
# MAGIC
# MAGIC 사용자별 격리는 조회할 때 `user_key` 필터를 붙여서 합니다. 행 필터가 걸린 테이블로는 인덱스를 만들 수 없기 때문에, 필터를 반드시 붙이는 책임은 조회하는 쪽 코드에 있습니다.

# COMMAND ----------

def pick_embedding_endpoint():
    """위젯에 값이 있으면 그대로 쓰고, 없으면 쓸 수 있는 모델 중 한국어에 유리한 순서로 고른다."""
    chosen = dbutils.widgets.get("embedding_endpoint").strip()
    if chosen:
        return chosen
    try:
        from databricks.sdk import WorkspaceClient

        names = {e.name for e in WorkspaceClient().serving_endpoints.list()}
    except Exception as e:
        print("엔드포인트 목록을 읽지 못해 기본값을 씁니다:", e)
        names = set()
    for candidate in ("databricks-qwen3-embedding-0-6b", "databricks-gte-large-en", "databricks-bge-large-en"):
        if candidate in names:
            return candidate
    return "databricks-gte-large-en"


def search_user(index, user_key, text="들른 곳", k=50):
    """한 사용자의 기억만 검색한다. user_key 필터를 항상 붙인다."""
    res = index.similarity_search(
        query_text=text,
        columns=["fact_id", "user_key", "fact_text"],
        filters={"user_key": user_key},
        num_results=k,
    )
    return res.get("result", {}).get("data_array") or []


def wait_until(predicate, timeout=1200, every=20):
    """조건이 참이 될 때까지 기다린다. 시간 안에 참이 되지 않으면 False."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(every)
    return False


index = None
if USE_VS:
    try:
        from databricks.ai_search.client import AISearchClient as SearchClient
    except ImportError:
        from databricks.vector_search.client import VectorSearchClient as SearchClient

    client = SearchClient(disable_notice=True)
    EMBEDDING_ENDPOINT = pick_embedding_endpoint()
    print("임베딩 모델:", EMBEDDING_ENDPOINT)

    try:
        client.wait_for_endpoint(VS_ENDPOINT)  # 이미 있으면 준비될 때까지 기다린다
    except Exception:
        client.create_endpoint_and_wait(name=VS_ENDPOINT, endpoint_type="STANDARD")

    try:
        index = client.get_index(endpoint_name=VS_ENDPOINT, index_name=INDEX)
        index.describe()
        # 이전 실행에서 남은 인덱스다. 지금의 골드 상태로 맞추고, 삭제 시험 대상의 기록이 다시 보일 때까지 기다린다.
        index.sync()
        wait_until(lambda: len(search_user(index, DELETE_USER)) > 0)
    except Exception:
        index = client.create_delta_sync_index_and_wait(
            endpoint_name=VS_ENDPOINT,
            source_table_name=GOLD,
            index_name=INDEX,
            pipeline_type="TRIGGERED",
            primary_key="fact_id",
            embedding_source_column="fact_text",
            embedding_model_endpoint_name=EMBEDDING_ENDPOINT,
        )
    print("인덱스 준비됨:", INDEX)
else:
    print("벡터 검색 단계를 건너뜁니다.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. 다음 날 치를 넣고 변경분만 조회
# MAGIC
# MAGIC 사건 30건(제안 10건이 모두 방문으로 끝남)과 정정 1건을 브론즈에 추가하고 야간 배치를 한 번 돌립니다. 골드에서 바뀐 것은 추가 10행과 수정 1행이어야 합니다.

# COMMAND ----------

v_before = gold_version()

to_df(day2_rows + [correction], BASE_DAY + timedelta(hours=42)).write.mode("append").saveAsTable(BRONZE)
nightly_batch()

changes = gold_changes(v_before)
check("3. 변경분만 조회됨", changes == {"insert": 10, "update_preimage": 1, "update_postimage": 1}, changes)

n_gold = spark.table(GOLD).count()
n_changed = changes.get("insert", 0) + changes.get("update_postimage", 0)
print(f"전체 {n_gold}행 중 다시 임베딩할 행은 {n_changed}행입니다 ({n_changed / n_gold:.0%}).")

display(
    spark.sql(
        f"SELECT _change_type, _commit_version, user_key, fact_text FROM table_changes('{GOLD}', {v_before + 1}) "
        "ORDER BY _change_type, fact_id"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. 벡터 인덱스 트리거 동기화
# MAGIC
# MAGIC `sync()`를 한 번 호출하면 지난 동기화 이후 바뀐 행만 처리합니다. 여기서는 새로 생긴 기억이 검색되는지로 확인합니다. 바뀐 행만 임베딩했는지는 실행 뒤에 `system.serving.endpoint_usage`의 입력 글자 수를 변경분의 글자 수와 대조해 볼 수 있습니다(2026-11-12 실행에서는 동기화 파이프라인 기록의 처리 행 수가 0으로만 남았습니다).

# COMMAND ----------

if index is not None:
    new_fact = next(
        r["event_id"] for r in day2_rows if r["event_type"] == "VISITED" and r["user_key"] == QUERY_USER and r["place_id"] == "p01"
    )
    index.sync()
    synced = wait_until(lambda: any(row[0] == new_fact for row in search_user(index, QUERY_USER)))
    check("4. 트리거 동기화 후 새 기억이 검색됨", synced, new_fact)
else:
    print("건너뜀")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. 두 가지 질문
# MAGIC
# MAGIC - "저번에 갔던 곳 어디였지?"는 **시간 순서**를 묻는 질문이라 날짜순 조회로 답합니다.
# MAGIC - "바다가 보이는 카페"는 **뜻**으로 찾는 질문이라 벡터 검색으로 답합니다.

# COMMAND ----------

latest = spark.sql(
    f"SELECT place_name, visited_on, fact_text FROM {GOLD} WHERE user_key = :u ORDER BY visited_at DESC LIMIT 1",
    args={"u": QUERY_USER},
).first()
truth = max(
    (r for r in day1_rows + day2_rows if r["event_type"] == "VISITED" and r["user_key"] == QUERY_USER),
    key=lambda r: r["event_ts"],
)
check('5. "저번에 갔던 곳" 날짜순 조회', latest["place_name"] == PLACE_NAME[truth["place_id"]], latest["fact_text"])

print("관계로 거르는 예: 동승자와 함께 간 볼 곳")
display(
    spark.sql(
        f"SELECT visited_on, place_name, region FROM {GOLD} "
        "WHERE user_key = :u AND companions AND place_type = '볼 곳' ORDER BY visited_at DESC",
        args={"u": QUERY_USER},
    )
)

# COMMAND ----------

if index is not None:
    question = "바다가 보이는 카페"
    hits = search_user(index, QUERY_USER, text=question, k=3)
    check("5. 뜻 검색 결과에 다른 사용자 기록이 섞이지 않음", len(hits) > 0 and all(h[1] == QUERY_USER for h in hits), f"{len(hits)}건")
    for h in hits:
        print(" -", h[2])
    print("참고: 상위 3건에 '파도소리 카페'가 있는가 →", any("파도소리 카페" in h[2] for h in hits))

    # 필터를 빼면 어떻게 되는지 보여 주는 대조 실험. 조회 코드가 필터를 강제해야 하는 이유다.
    unfiltered = index.similarity_search(query_text=question, columns=["fact_id", "user_key", "fact_text"], num_results=10)
    owners = {row[1] for row in unfiltered.get("result", {}).get("data_array") or []}
    print(f"참고: 필터 없이 검색하면 상위 10건에 {len(owners)}명의 기록이 섞여 나옵니다.")
else:
    print("건너뜀")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. 삭제가 끝까지 전달되는지
# MAGIC
# MAGIC 동의를 철회한 사용자의 기록을 브론즈에서 지우고 야간 배치를 돌리면, 실버와 골드에서도 빠지고 CDF에 삭제로 기록됩니다. 인덱스는 다음 동기화에서 그 행을 뺍니다.
# MAGIC
# MAGIC 파일에서 완전히 지우려면 운영에서는 `REORG TABLE ... APPLY (PURGE)`와 `VACUUM`까지 해야 합니다. 이 노트북은 거기까지 하지 않습니다.

# COMMAND ----------

n_before = spark.sql(f"SELECT count(*) AS n FROM {GOLD} WHERE user_key = :u", args={"u": DELETE_USER}).first()["n"]
v_before = gold_version()

spark.sql(f"DELETE FROM {BRONZE} WHERE user_key = :u", args={"u": DELETE_USER})
nightly_batch()

n_after = spark.sql(f"SELECT count(*) AS n FROM {GOLD} WHERE user_key = :u", args={"u": DELETE_USER}).first()["n"]
changes = gold_changes(v_before)
check("6. 삭제가 골드까지 전달됨", n_before > 0 and n_after == 0 and changes == {"delete": n_before}, f"{n_before}행 → {n_after}행, 변경분 {changes}")

if index is not None:
    index.sync()
    gone = wait_until(lambda: len(search_user(index, DELETE_USER)) == 0)
    check("6. 삭제가 인덱스까지 전달됨", gone)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 결과

# COMMAND ----------

display(spark.createDataFrame(results, "check STRING, result STRING, detail STRING"))
n_fail = sum(1 for r in results if r[1] == "FAIL")
print(f"{len(results)}개 중 {len(results) - n_fail}개 통과, {n_fail}개 실패")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 정리
# MAGIC
# MAGIC 벡터 검색 엔드포인트는 인덱스가 있는 동안 계속 과금 대상입니다(유료 작업 공간 기준). 시험이 끝나면 아래 셀의 주석을 풀어 인덱스와 엔드포인트를 지웁니다. 지우기 전에 이름이 이 노트북에서 만든 것이 맞는지 확인합니다.

# COMMAND ----------

# print("지울 대상:", INDEX, "/", VS_ENDPOINT)
# client.delete_index(endpoint_name=VS_ENDPOINT, index_name=INDEX)
# client.delete_endpoint(VS_ENDPOINT)

# COMMAND ----------

# 작업(Job)으로 실행하면 셀 출력을 밖에서 읽을 수 없으므로, 확인 결과를 실행 출력으로 돌려준다.
# 화면에서 직접 실행할 때는 여기서 노트북이 끝난다.
import json

dbutils.notebook.exit(json.dumps({"results": results, "n_fail": n_fail}, ensure_ascii=False))
