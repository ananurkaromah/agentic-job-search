-- ============================================================
-- Lakebase Schema: AI Job Hunting Copilot
-- Target: Lakebase (Postgres-compatible) project, e.g. `job_copilot`
-- Run these statements from a Lakebase SQL editor / psql connection,
-- or via a Databricks notebook using the Lakebase JDBC/psycopg2 connection.
-- ============================================================

CREATE SCHEMA IF NOT EXISTS job_copilot;
SET search_path TO job_copilot;

-- pgvector: all semantic search (job postings + resumes) goes through
-- this extension directly -- no Databricks Vector Search, no Delta sync.
CREATE EXTENSION IF NOT EXISTS vector;

-- 1. users -----------------------------------------------------
CREATE TABLE IF NOT EXISTS users (
    user_id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name                TEXT NOT NULL,
    email               TEXT NOT NULL UNIQUE,
    target_roles        TEXT[],                 -- e.g. {'Data Engineer','Analytics Engineer'}
    target_salary_min   NUMERIC(12,2),
    preferred_locations TEXT[],                 -- e.g. {'Remote','Austin, TX'}
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 2. profiles ----------------------------------------------------
CREATE TABLE IF NOT EXISTS profiles (
    profile_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id             UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    resume_text         TEXT,                   -- raw resume text, embedded by Vector Search
    primary_skills      TEXT[],
    experience_summary  TEXT,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_profiles_user_id ON profiles(user_id);

-- 3. skills --------------------------------------------------------
CREATE TABLE IF NOT EXISTS skills (
    skill_id            SERIAL PRIMARY KEY,
    name                TEXT NOT NULL UNIQUE,
    category            TEXT,                   -- e.g. 'language','platform','tool'
    proficiency_level   TEXT                    -- e.g. 'beginner','intermediate','advanced'
);

-- 4. job_postings ----------------------------------------------------
CREATE TABLE IF NOT EXISTS job_postings (
    job_id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_api          TEXT NOT NULL CHECK (source_api IN ('adzuna','usajobs','remoteok')),
    external_id         TEXT NOT NULL,
    title               TEXT NOT NULL,
    company             TEXT,
    location            TEXT,
    salary_range        TEXT,                   -- normalized display string, e.g. "$110k-$140k"
    raw_description     TEXT,
    clean_description   TEXT,                   -- cleaned text, source for embeddings
    posted_at           TIMESTAMPTZ,
    vector_id           TEXT,                   -- id/key in the Vector Search index
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (source_api, external_id)
);
CREATE INDEX IF NOT EXISTS idx_job_postings_title ON job_postings(title);

-- 5. applications -----------------------------------------------------
CREATE TABLE IF NOT EXISTS applications (
    application_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id             UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    job_id              UUID NOT NULL REFERENCES job_postings(job_id) ON DELETE CASCADE,
    stage               TEXT NOT NULL DEFAULT 'saved'
                         CHECK (stage IN ('saved','applied','interviewing','rejected','offer')),
    applied_date         DATE,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (user_id, job_id)
);
CREATE INDEX IF NOT EXISTS idx_applications_user_stage ON applications(user_id, stage);

-- 6. saved_jobs -------------------------------------------------------
CREATE TABLE IF NOT EXISTS saved_jobs (
    saved_id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id             UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    job_id              UUID NOT NULL REFERENCES job_postings(job_id) ON DELETE CASCADE,
    saved_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    notes               TEXT,
    UNIQUE (user_id, job_id)
);

-- 7. interview_notes --------------------------------------------------
CREATE TABLE IF NOT EXISTS interview_notes (
    note_id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    application_id      UUID NOT NULL REFERENCES applications(application_id) ON DELETE CASCADE,
    stage               TEXT,                   -- stage at time the note was taken
    note_text           TEXT NOT NULL,
    follow_up_date      DATE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_interview_notes_app ON interview_notes(application_id);

-- 8. contacts -----------------------------------------------------------
CREATE TABLE IF NOT EXISTS contacts (
    contact_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    company_name        TEXT,
    name                TEXT NOT NULL,
    role                TEXT,
    email               TEXT,
    linkedin_url        TEXT,
    application_id      UUID REFERENCES applications(application_id) ON DELETE SET NULL
);

-- 9. job_documents -------------------------------------------------------
-- Source text for embedding: one row per job posting or resume, populated
-- by embeddings/sync_documents.py directly from job_postings/profiles
-- (plain SQL, same database -- no Spark, no Delta involved).
CREATE TABLE IF NOT EXISTS job_documents (
    id                  TEXT PRIMARY KEY,       -- job_postings.job_id or profiles.profile_id, as text
    source_type         TEXT NOT NULL,          -- 'job_posting' or 'resume'
    description_text    TEXT,
    content_hash        TEXT NOT NULL,          -- md5 of description_text; drives re-embedding
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 10. job_embeddings ------------------------------------------------------
-- Chunked, embedded text for pgvector similarity search. Populated by
-- embeddings/ingest_job_embeddings.py. all-MiniLM-L6-v2 -> 384 dimensions.
CREATE TABLE IF NOT EXISTS job_embeddings (
    id                  BIGSERIAL PRIMARY KEY,
    document_id         TEXT NOT NULL REFERENCES job_documents(id) ON DELETE CASCADE,
    source_type         TEXT NOT NULL,
    chunk_index         INTEGER NOT NULL,
    chunk_text          TEXT NOT NULL,
    content_hash        TEXT NOT NULL,
    embedding           VECTOR(384) NOT NULL,
    model_name          TEXT NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (document_id, chunk_index)
);
CREATE INDEX IF NOT EXISTS idx_job_embeddings_document_id ON job_embeddings(document_id);
CREATE INDEX IF NOT EXISTS idx_job_embeddings_content_hash ON job_embeddings(content_hash);
