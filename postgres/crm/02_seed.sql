INSERT INTO crm_users (user_id, prosthesis_id, client_name, email, country, updated_at)
VALUES
  ('u-001', 'p-1001', 'Иван Петров', 'ivan.petrov@example.com', 'RU', NOW()),
  ('u-002', 'p-1002', 'Мария Смирнова', 'maria.smirnova@example.com', 'RU', NOW()),
  ('u-003', 'p-2001', 'John Doe', 'john.doe@example.com', 'EU', NOW())
ON CONFLICT (user_id, prosthesis_id) DO UPDATE
SET client_name = EXCLUDED.client_name,
    email       = EXCLUDED.email,
    country     = EXCLUDED.country,
    updated_at  = EXCLUDED.updated_at;
