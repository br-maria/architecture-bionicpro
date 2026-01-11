CREATE TABLE IF NOT EXISTS telemetry_events (
  event_id       BIGSERIAL PRIMARY KEY,
  event_ts       TIMESTAMP NOT NULL,
  user_id        TEXT NOT NULL,
  prosthesis_id  TEXT NOT NULL,
  event_type     TEXT NOT NULL,         -- movement | error | other
  signal_level   DOUBLE PRECISION,      -- условный уровень сигнала
  noise_level    DOUBLE PRECISION,      -- условный уровень шума
  battery_level  INTEGER                -- 0..100
);

CREATE INDEX IF NOT EXISTS idx_tel_ts ON telemetry_events(event_ts);
CREATE INDEX IF NOT EXISTS idx_tel_user_ts ON telemetry_events(user_id, event_ts);
CREATE INDEX IF NOT EXISTS idx_tel_user_prosthesis_ts ON telemetry_events(user_id, prosthesis_id, event_ts);
