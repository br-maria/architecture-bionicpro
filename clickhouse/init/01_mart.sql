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
