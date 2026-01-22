-- Агрегированная витрина по телеметрии с шагом 1 час.
-- Нужна, чтобы ETL в Airflow не считал агрегации в момент загрузки в OLAP.

DROP TABLE IF EXISTS telemetry_hourly;

CREATE TABLE telemetry_hourly (
  period_start         timestamp without time zone NOT NULL,
  user_id              text NOT NULL,
  prosthesis_id        text NOT NULL,

  telemetry_events_count integer NOT NULL,
  movements_count        integer NOT NULL,
  errors_count           integer NOT NULL,
  battery_low_events     integer NOT NULL,

  avg_signal_level       double precision,
  avg_noise_level        double precision,

  PRIMARY KEY (period_start, user_id, prosthesis_id)
);

INSERT INTO telemetry_hourly (
  period_start,
  user_id,
  prosthesis_id,
  telemetry_events_count,
  movements_count,
  errors_count,
  battery_low_events,
  avg_signal_level,
  avg_noise_level
)
SELECT
  date_trunc('hour', event_ts)                                         AS period_start,
  user_id,
  prosthesis_id,
  count(*)::int                                                        AS telemetry_events_count,
  sum(CASE WHEN event_type = 'movement' THEN 1 ELSE 0 END)::int        AS movements_count,
  sum(CASE WHEN event_type = 'error' THEN 1 ELSE 0 END)::int           AS errors_count,
  sum(CASE WHEN battery_level IS NOT NULL AND battery_level < 20 THEN 1 ELSE 0 END)::int
                                                                       AS battery_low_events,
  avg(signal_level)                                                    AS avg_signal_level,
  avg(noise_level)                                                     AS avg_noise_level
FROM telemetry_events
GROUP BY 1, 2, 3;

CREATE INDEX IF NOT EXISTS idx_telemetry_hourly_user_period
  ON telemetry_hourly (user_id, period_start);
