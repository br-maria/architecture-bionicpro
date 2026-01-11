ATTACH TABLE _ UUID '31546079-f3d0-4fdc-9106-d84f2116e305'
(
    `period_start` DateTime,
    `period_end` DateTime,
    `user_id` String,
    `prosthesis_id` String,
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
