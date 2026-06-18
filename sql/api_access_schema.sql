BEGIN;

CREATE SCHEMA IF NOT EXISTS api_access;

CREATE TABLE IF NOT EXISTS api_access.api_keys (
  id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  name          TEXT NOT NULL,
  key_prefix    TEXT NOT NULL UNIQUE,
  key_hash      TEXT NOT NULL UNIQUE,
  is_active     BOOLEAN NOT NULL DEFAULT TRUE,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_used_at  TIMESTAMPTZ,
  revoked_at    TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_api_keys_active
  ON api_access.api_keys(is_active, created_at DESC);

COMMIT;
