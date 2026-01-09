from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.hooks.http import HttpHook


DAG_ID = "reports_etl_hourly"


def ch_execute(sql: str, conn_id: str = "clickhouse_http") -> None:
    """
    Выполняем SQL в ClickHouse через HTTP interface.
    В Airflow connection clickhouse_http должен быть настроен host:port.
    """
    hook = HttpHook(method="POST", http_conn_id=conn_id)
    # ClickHouse принимает SQL в теле запроса
    resp = hook.run(
        endpoint="/",
        data=sql.encode("utf-8"),
        headers={"Content-Type": "text/plain; charset=utf-8"},
        extra_options={"timeout": 60},
    )
    if resp.status_code >= 300:
        raise RuntimeError(f"ClickHouse error {resp.status_code}: {resp.text}")


def ensure_tables():
    # Витрина
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

    # staging CRM (текущая “снимковая” таблица)
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

    # staging телеметрии за окно (почасовые агрегаты)
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


def load_crm_to_clickhouse():
    """
    Забираем пользователей/протезы из CRM и грузим в stg_crm_users.
    Важно: CRM может быть не Postgres; тогда заменить hook/источник.
    """
    crm = PostgresHook(postgres_conn_id="crm_postgres")

    # Примерный запрос: адаптируй под реальные таблицы CRM-реплики
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

    # Грузим батчами в ClickHouse
    if not rows:
        return

    values = ",".join([
        "('{user_id}','{prosthesis_id}','{client_name}','{email}','{country}','{updated_at}')".format(
            user_id=str(r[0]).replace("'", "\\'"),
            prosthesis_id=str(r[1]).replace("'", "\\'"),
            client_name=str(r[2]).replace("'", "\\'"),
            email=str(r[3]).replace("'", "\\'"),
            country=str(r[4]).replace("'", "\\'"),
            updated_at=str(r[5]),
        )
        for r in rows
    ])

    ch_execute(f"""
        INSERT INTO stg_crm_users (user_id, prosthesis_id, client_name, email, country, updated_at)
        VALUES {values};
    """)


def load_telemetry_hourly_to_clickhouse(**context):
    """
    Агрегируем телеметрию за предыдущий час и грузим в stg_telemetry_hourly.
    Окно: [data_interval_start, data_interval_end)
    """
    start = context["data_interval_start"]
    end = context["data_interval_end"]

    pg = PostgresHook(postgres_conn_id="telemetry_postgres")

    # Пример: заменяй названия таблиц/полей на свои
    # Важно: группируем по user_id + prosthesis_id → “в разрезе клиентов”
    rows = pg.get_records("""
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
    """, parameters={"start": start, "end": end})

    # Перед загрузкой удалим данные за это окно (идемпотентность)
    ch_execute(f"""
      ALTER TABLE stg_telemetry_hourly
      DELETE WHERE period_start = toDateTime('{start}') AND period_end = toDateTime('{end}');
    """)

    if not rows:
        return

    values = ",".join([
        "('{ps}','{pe}','{user_id}','{prosthesis_id}',{cnt},{mov},{sig},{noise},{err},{bat})".format(
            ps=str(r[0]),
            pe=str(r[1]),
            user_id=str(r[2]).replace("'", "\\'"),
            prosthesis_id=str(r[3]).replace("'", "\\'"),
            cnt=int(r[4]),
            mov=int(r[5]),
            sig=float(r[6]) if r[6] is not None else 0.0,
            noise=float(r[7]) if r[7] is not None else 0.0,
            err=int(r[8]),
            bat=int(r[9]),
        )
        for r in rows
    ])

    ch_execute(f"""
        INSERT INTO stg_telemetry_hourly
        (period_start, period_end, user_id, prosthesis_id,
         telemetry_events_count, movements_count, avg_signal_level, avg_noise_level, errors_count, battery_low_events)
        VALUES {values};
    """)


def build_mart_for_window(**context):
    """
    Собираем витрину: join stg_telemetry_hourly + stg_crm_users → report_mart_user_telemetry_hourly.
    """
    start = context["data_interval_start"]
    end = context["data_interval_end"]

    # Идемпотентность: удалим витринные строки за окно
    ch_execute(f"""
      ALTER TABLE report_mart_user_telemetry_hourly
      DELETE WHERE period_start = toDateTime('{start}') AND period_end = toDateTime('{end}');
    """)

    # Вставка join’ом внутри ClickHouse
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
      WHERE t.period_start = toDateTime('{start}') AND t.period_end = toDateTime('{end}');
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
    schedule_interval="0 * * * *",  # каждый час в 00 минут
    catchup=False,
    max_active_runs=1,
    tags=["reports", "etl", "clickhouse"],
) as dag:

    t0 = PythonOperator(task_id="ensure_tables", python_callable=ensure_tables)

    t1 = PythonOperator(task_id="extract_crm_to_staging", python_callable=load_crm_to_clickhouse)

    t2 = PythonOperator(
        task_id="aggregate_telemetry_hourly_to_staging",
        python_callable=load_telemetry_hourly_to_clickhouse,
        provide_context=True,
    )

    t3 = PythonOperator(
        task_id="build_report_mart_for_window",
        python_callable=build_mart_for_window,
        provide_context=True,
    )

    t0 >> [t1, t2] >> t3
