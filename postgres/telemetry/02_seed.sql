-- Сгенерируем события за последние 3 часа для трёх пользователей

INSERT INTO telemetry_events (event_ts, user_id, prosthesis_id, event_type, signal_level, noise_level, battery_level)
SELECT
  NOW() - (g.i || ' minutes')::interval AS event_ts,
  g.user_id,
  g.prosthesis_id,
  CASE WHEN g.i % 17 = 0 THEN 'error'
       WHEN g.i % 3  = 0 THEN 'movement'
       ELSE 'other' END AS event_type,
  50 + (g.i % 20) * 1.0 AS signal_level,
  10 + (g.i % 7)  * 1.0 AS noise_level,
  100 - (g.i % 30) AS battery_level
FROM (
  SELECT i, 'u-001'::text AS user_id, 'p-1001'::text AS prosthesis_id FROM generate_series(1, 120) AS i
  UNION ALL
  SELECT i, 'u-002'::text AS user_id, 'p-1002'::text AS prosthesis_id FROM generate_series(1, 90) AS i
  UNION ALL
  SELECT i, 'u-003'::text AS user_id, 'p-2001'::text AS prosthesis_id FROM generate_series(1, 60) AS i
) g;
