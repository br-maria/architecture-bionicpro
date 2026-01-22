from __future__ import annotations

from datetime import timedelta
from typing import Optional, Tuple

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook


DAG_ID = "reports_etl_hourly"

# conn_id должны соответствовать переменным окружения вида AIRFLOW_CONN_<CONN_ID>.
# Важно: Airflow приводит имя коннекта к нижнему регистру.
# Пример: AIRFLOW_CONN_CRM_DB -> conn_id "crm_db".
CLICKHOUSE_CONN_ID = "clickhouse_http"
CRM_CONN_ID = "crm_db"
TELEMETRY_CONN_ID = "telemetry_db"

CLICKHOUSE_DATABASE: Optional[str] = None  # например: "reports"


def _esc_ch(v: object) -> str:
    """
    Экранирует строковое значение для вставки через VALUES.
    Для учебного проекта достаточно. Для больших объёмов лучше CSV/JSONEachRow.
    """
    return str(v).replace("\\", "\\\\").replace("'", "\\'")


def _ch_dt(ts: str) -> str:
    """
    Нормализует строку времени для ClickHouse DateTime/toDateTime():
    - заменяет 'T' на пробел, убирает 'Z'
    - убирает timezone суффиксы вида '+00:00' / '-03:00'
    - обрезает микросекунды '.123456'
    Возвращает 'YYYY-MM-DD HH:MM:SS'
    """
    s = str(ts).strip()
    s = s.replace("T", " ").replace("Z", "").strip()

    # Убираем timezone, если есть (обычно +00:00)
    if "+" in s:
        s = s.split("+", 1)[0].strip()

    # Убираем timezone с минусом (после времени), не трогая минусы в дате
    # Если после позиции 19 (YYYY-MM-DD HH:MM:SS) остался '-', это скорее всего TZ.
    if len(s) > 19 and "-" in s[19:]:
        s = s.split("-", 1)[0].strip()

    # Обрезаем микросекунды
    if "." in s:
        s = s.split(".", 1)[0].strip()

    return s


def ch_execute(sql: str, conn_id: str = CLICKHOUSE_CONN_ID) -> None:
    """
    Выполняет SQL в ClickHouse через HTTP.

    Ожидания по connection clickhouse_http:
      - schema: http
      - host: clickhouse
      - port: 8123
      - login/password: пользователь ClickHouse
    """
    hook = HttpHook(method="POST", http_conn_id=conn_id)

    endpoint = "/"
    if CLICKHOUSE_DATABASE:
        endpoint = f"/?database={CLICKHOUSE_DATABASE}"

    resp = hook.run(
        endpoint=endpoint,
        data=sql.encode("utf-8"),
        headers={"Content-Type": "text/plain; charset=utf-8"},
        extra_options={"timeout": 120},
    )

    if resp.status_code >= 300:
        raise RuntimeError(f"ClickHouse error {resp.status_code}: {resp.text}")


def ensure_tables() -> None:
    """
    Гарантирует наличие таблиц в ClickHouse.

    stg_crm_users:
      - снимок пользователей/протезов из CRM
    stg_telemetry_hourly:
      - почасовая агрегация телеметрии
    report_mart_user_telemetry_hourly:
      - витрина для отчётов (то, что читает API)
    """
    ch_execute("""
    CREATE TABLE IF NOT EXISTS stg_crm_users
    (
      user_id String,
      prosthesis_id String,
      client_name String,
      email String,
      country String,
      updated_at DateTime
    )
    ENGINE = ReplacingMergeTree(updated_at)
    ORDER BY (user_id, prosthesis_id);
    """)

    ch_execute("""
    CREATE TABLE IF NOT EXISTS stg_telemetry_hourly
    (
      period_start DateTime,
      period_end   DateTime,
      user_id      String,
      prosthesis_id String,
      telemetry_events_count UInt64,
      movements_count        UInt64,
      avg_signal_level       Float64,
      avg_noise_level        Float64,
      errors_count           UInt64,
      battery_low_events     UInt64
    )
    ENGINE = MergeTree
    PARTITION BY toYYYYMM(period_start)
    ORDER BY (user_id, prosthesis_id, period_start);
    """)

    ch_execute("""
    CREATE TABLE IF NOT EXISTS report_mart_user_telemetry_hourly
    (
      period_start DateTime,
      period_end   DateTime,
      user_id      String,
      prosthesis_id String,
      crm_client_name String,
      crm_email       String,
      crm_country     String,
      telemetry_events_count UInt64,
      movements_count        UInt64,
      avg_signal_level       Float64,
      avg_noise_level        Float64,
      errors_count           UInt64,
      battery_low_events     UInt64
    )
    ENGINE = MergeTree
    PARTITION BY toYYYYMM(period_start)
    ORDER BY (user_id, prosthesis_id, period_start);
    """)


def load_crm_to_clickhouse() -> None:
    """
    Забирает данные из CRM Postgres и загружает их в ClickHouse stg_crm_users.

    Требования к CRM:
      - таблица crm_users существует
      - поля user_id, prosthesis_id, client_name, email, country, updated_at существуют
    """
    crm = PostgresHook(postgres_conn_id=CRM_CONN_ID)

    rows = crm.get_records("""
        SELECT
          user_id::text,
          prosthesis_id::text,
          client_name::text,
          email::text,
          country::text,
          updated_at
        FROM crm_users
    """)

    if not rows:
        return

    values = ",".join(
        "('{user_id}','{prosthesis_id}','{client_name}','{email}','{country}','{updated_at}')".format(
            user_id=_esc_ch(r[0]),
            prosthesis_id=_esc_ch(r[1]),
            client_name=_esc_ch(r[2]),
            email=_esc_ch(r[3]),
            country=_esc_ch(r[4]),
            # ВАЖНО: нормализуем до 'YYYY-MM-DD HH:MM:SS'
            updated_at=_esc_ch(_ch_dt(str(r[5]))),
        )
        for r in rows
    )

    ch_execute(f"""
        INSERT INTO stg_crm_users (user_id, prosthesis_id, client_name, email, country, updated_at)
        VALUES {values};
    """)


def _resolve_window(context) -> Tuple[str, str]:
    """
    Определяет окно агрегации.

    По умолчанию используем data_interval_start/end от Airflow.
    Для ручного теста можно передать диапазон через dag_run.conf:
      {"start_ts": "...", "end_ts": "..."}
    Значения должны быть строками в ISO-формате.
    """
    dag_run = context.get("dag_run")
    conf = (dag_run.conf or {}) if dag_run else {}

    start_ts = conf.get("start_ts")
    end_ts = conf.get("end_ts")

    if start_ts and end_ts:
        return str(start_ts), str(end_ts)

    return str(context["data_interval_start"]), str(context["data_interval_end"])


def load_telemetry_hourly_to_clickhouse(**context) -> None:
    """
    Агрегирует telemetry_events за окно и пишет результат в stg_telemetry_hourly.

    Типовая причина "всё пусто": события в telemetry_events не попадают в последний час,
    а DAG запускается по расписанию. Для теста запускай DAG вручную с start_ts/end_ts.
    """
    start_ts, end_ts = _resolve_window(context)
    start_ch, end_ch = _ch_dt(start_ts), _ch_dt(end_ts)

    pg = PostgresHook(postgres_conn_id=TELEMETRY_CONN_ID)

    rows = pg.get_records(
        """
        SELECT
          %(start)s::timestamp as period_start,
          %(end)s::timestamp   as period_end,
          user_id::text,
          prosthesis_id::text,
          COUNT(*)::bigint as telemetry_events_count,
          SUM(CASE WHEN event_type='movement' THEN 1 ELSE 0 END)::bigint as movements_count,
          AVG(signal_level)::float as avg_signal_level,
          AVG(noise_level)::float  as avg_noise_level,
          SUM(CASE WHEN event_type='error' THEN 1 ELSE 0 END)::bigint as errors_count,
          SUM(CASE WHEN battery_level < 15 THEN 1 ELSE 0 END)::bigint as battery_low_events
        FROM telemetry_events
        WHERE event_ts >= %(start)s AND event_ts < %(end)s
        GROUP BY user_id, prosthesis_id
        """,
        parameters={"start": start_ts, "end": end_ts},
    )

    # Делаем повторный прогон окна безопасным.
    ch_execute(f"""
      ALTER TABLE stg_telemetry_hourly
      DELETE WHERE period_start = toDateTime('{start_ch}') AND period_end = toDateTime('{end_ch}');
    """)

    if not rows:
        return

    values = ",".join(
        "('{ps}','{pe}','{user_id}','{prosthesis_id}',{cnt},{mov},{sig},{noise},{err},{bat})".format(
            # нормализуем period_start/end, чтобы гарантированно без микросекунд
            ps=_esc_ch(_ch_dt(str(r[0]))),
            pe=_esc_ch(_ch_dt(str(r[1]))),
            user_id=_esc_ch(r[2]),
            prosthesis_id=_esc_ch(r[3]),
            cnt=int(r[4]),
            mov=int(r[5]),
            sig=float(r[6]) if r[6] is not None else 0.0,
            noise=float(r[7]) if r[7] is not None else 0.0,
            err=int(r[8]),
            bat=int(r[9]),
        )
        for r in rows
    )

    ch_execute(f"""
        INSERT INTO stg_telemetry_hourly
        (period_start, period_end, user_id, prosthesis_id,
         telemetry_events_count, movements_count, avg_signal_level, avg_noise_level, errors_count, battery_low_events)
        VALUES {values};
    """)


def build_mart_for_window(**context) -> None:
    """
    Строит витрину за окно: stg_telemetry_hourly + stg_crm_users -> report_mart_user_telemetry_hourly.
    """
    start_ts, end_ts = _resolve_window(context)
    start_ch, end_ch = _ch_dt(start_ts), _ch_dt(end_ts)

    ch_execute(f"""
      ALTER TABLE report_mart_user_telemetry_hourly
      DELETE WHERE period_start = toDateTime('{start_ch}') AND period_end = toDateTime('{end_ch}');
    """)

    ch_execute(f"""
      INSERT INTO report_mart_user_telemetry_hourly
      SELECT
        t.period_start,
        t.period_end,
        t.user_id,
        t.prosthesis_id,
        c.client_name as crm_client_name,
        c.email       as crm_email,
        c.country     as crm_country,
        t.telemetry_events_count,
        t.movements_count,
        t.avg_signal_level,
        t.avg_noise_level,
        t.errors_count,
        t.battery_low_events
      FROM stg_telemetry_hourly t
      LEFT JOIN stg_crm_users c
        ON c.user_id = t.user_id AND c.prosthesis_id = t.prosthesis_id
      WHERE t.period_start = toDateTime('{start_ch}') AND t.period_end = toDateTime('{end_ch}');
    """)


default_args = {
    "owner": "bionicpro",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="0 * * * *",
    catchup=False,
    max_active_runs=10,
    tags=["reports", "etl", "clickhouse"],
) as dag:
    ensure = PythonOperator(
        task_id="ensure_tables",
        python_callable=ensure_tables,
    )

    crm_to_ch = PythonOperator(
        task_id="extract_crm_to_staging",
        python_callable=load_crm_to_clickhouse,
    )

    telemetry_to_ch = PythonOperator(
        task_id="aggregate_telemetry_hourly_to_staging",
        python_callable=load_telemetry_hourly_to_clickhouse,
    )

    mart = PythonOperator(
        task_id="build_report_mart_for_window",
        python_callable=build_mart_for_window,
    )

    ensure >> [crm_to_ch, telemetry_to_ch] >> mart
