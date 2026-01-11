ATTACH TABLE _ UUID '4186030d-91e1-4357-8826-4cbad46823a9'
(
    `period_start` DateTime,
    `period_end` DateTime,
    `user_id` String,
    `prosthesis_id` String,
    `crm_client_name` String,
    `crm_email` String,
    `crm_country` String,
    `telemetry_events_count` UInt64,
    `movements_count` UInt64,
    `avg_signal_level` Float64,
    `avg_noise_level` Float64,
    `errors_count` UInt64,
    `battery_low_events` UInt64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(period_start)
ORDER BY (user_id, prosthesis_id, period_start)
SETTINGS index_granularity = 8192
