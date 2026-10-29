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
-- Persistent configuration is independent of expiring quota debt.
CREATE TABLE IF NOT EXISTS pacelock_policies (
    scope_id text NOT NULL REFERENCES pacelock_domains(scope_id),
    quota_id text NOT NULL,
    interval_us bigint NOT NULL CHECK (interval_us >= 0),
    burst bigint NOT NULL CHECK (burst > 0),
    revision bigint NOT NULL CHECK (revision >= 1 AND revision <= 9007199254740991),
    PRIMARY KEY (scope_id, quota_id)
);

-- Both times must have recovered. This index skips active debt during pruning.
CREATE INDEX IF NOT EXISTS pacelock_quotas_recovered_idx
 ON pacelock_quotas (scope_id, (GREATEST(expires_us, tat_us)), quota_id);
