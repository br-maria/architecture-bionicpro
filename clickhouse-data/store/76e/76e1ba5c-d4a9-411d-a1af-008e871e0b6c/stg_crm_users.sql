ATTACH TABLE _ UUID 'af2cbf54-9f88-4954-82a0-5f2e803c82c1'
(
    `user_id` String,
    `prosthesis_id` String,
    `client_name` String,
    `email` String,
    `country` String,
    `updated_at` DateTime
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (user_id, prosthesis_id)
SETTINGS index_granularity = 8192
