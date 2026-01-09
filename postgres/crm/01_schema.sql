CREATE TABLE IF NOT EXISTS crm_users (
  user_id        TEXT NOT NULL,
  prosthesis_id  TEXT NOT NULL,
  client_name    TEXT NOT NULL,
  email          TEXT,
  country        TEXT,
  updated_at     TIMESTAMP NOT NULL DEFAULT NOW(),
  PRIMARY KEY (user_id, prosthesis_id)
);

CREATE INDEX IF NOT EXISTS idx_crm_users_user ON crm_users(user_id);
CREATE INDEX IF NOT EXISTS idx_crm_users_prosthesis ON crm_users(prosthesis_id);
