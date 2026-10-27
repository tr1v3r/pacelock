-- Fixed, logged tables. Call Migrate explicitly with a migration-capable role.
CREATE TABLE IF NOT EXISTS pacelock_domains (
    scope_id text PRIMARY KEY
);
CREATE TABLE IF NOT EXISTS pacelock_quotas (
    scope_id text NOT NULL REFERENCES pacelock_domains(scope_id),
    quota_id text NOT NULL,
    tat_us bigint NOT NULL CHECK (tat_us >= 0),
    interval_us bigint NOT NULL CHECK (interval_us > 0),
    burst bigint NOT NULL CHECK (burst > 0),
    expires_us bigint NOT NULL,
    PRIMARY KEY (scope_id, quota_id)
);
CREATE TABLE IF NOT EXISTS pacelock_admissions (
    scope_id text NOT NULL REFERENCES pacelock_domains(scope_id),
    admission_id text NOT NULL,
    fingerprint text NOT NULL,
    decision bytea NOT NULL,
    expires_us bigint NOT NULL,
    PRIMARY KEY (scope_id, admission_id)
);
