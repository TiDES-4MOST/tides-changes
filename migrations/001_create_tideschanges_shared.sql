-- Migration: 001_create_tideschanges_shared.sql
--
-- Creates the tideschanges_shared bookkeeping table in the tides schema.
--
-- This table is written by the CHANGES ingest flow (tides-changes) and may
-- also be read/written by the TiDES ingest flow (tides-ingest) when checking
-- whether a transient was already triggered by CHANGES.
--
-- Apply with:
--   psql $TIDES_DB_URL -f migrations/001_create_tideschanges_shared.sql

BEGIN;

CREATE TABLE IF NOT EXISTS tides.tideschanges_shared (
    id                  SERIAL PRIMARY KEY,

    -- Transient identification
    transient_name      VARCHAR(255) NOT NULL,
    ra                  DOUBLE PRECISION,
    dec                 DOUBLE PRECISION,

    -- TiDES bookkeeping
    tides_triggered     BOOLEAN NOT NULL DEFAULT FALSE,
    tides_triggered_at  TIMESTAMPTZ,

    -- CHANGES bookkeeping
    changes_triggered     BOOLEAN NOT NULL DEFAULT FALSE,
    changes_triggered_at  TIMESTAMPTZ,

    -- Derived: which survey triggered first ('tides' or 'changes')
    first_trigger       VARCHAR(50),

    -- Whether both surveys have triggered this transient
    is_shared           BOOLEAN NOT NULL DEFAULT FALSE,

    -- 4MOST submission outcome
    survey_submitted_as VARCHAR(50),   -- 'TiDES' or 'CHANGES'
    pk_4most            INTEGER,
    ostd_u_obj_id       VARCHAR(255),
    submitted_at        TIMESTAMPTZ,

    -- Row metadata
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_tideschanges_shared_name UNIQUE (transient_name)
);

-- Index to speed up name-based lookups from both flows
CREATE INDEX IF NOT EXISTS idx_tideschanges_shared_name
    ON tides.tideschanges_shared (transient_name);

-- Index to find all shared targets quickly
CREATE INDEX IF NOT EXISTS idx_tideschanges_shared_is_shared
    ON tides.tideschanges_shared (is_shared);

COMMIT;
