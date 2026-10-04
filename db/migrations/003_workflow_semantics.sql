BEGIN;

ALTER TABLE workflows ADD COLUMN definition_hash text;
UPDATE workflows SET definition_hash = encode(digest(id::text, 'sha256'), 'hex');
ALTER TABLE workflows ALTER COLUMN definition_hash SET NOT NULL;
ALTER TABLE workflows ADD CONSTRAINT workflows_definition_hash_check CHECK (length(definition_hash) = 64);

ALTER TABLE steps DROP CONSTRAINT steps_kind_check;
ALTER TABLE steps ADD CONSTRAINT steps_kind_check CHECK (kind IN ('echo', 'noop', 'sleep', 'flaky', 'fail'));
ALTER TABLE steps ADD COLUMN max_attempts integer NOT NULL DEFAULT 3 CHECK (max_attempts BETWEEN 1 AND 10);
ALTER TABLE steps ADD COLUMN retry_backoff_ms bigint NOT NULL DEFAULT 250 CHECK (retry_backoff_ms BETWEEN 1 AND 3600000);
ALTER TABLE steps ADD COLUMN timeout_ms bigint NOT NULL DEFAULT 30000 CHECK (timeout_ms BETWEEN 1 AND 300000);

ALTER TABLE attempts DROP CONSTRAINT attempts_status_check;
ALTER TABLE attempts ADD CONSTRAINT attempts_status_check
    CHECK (status IN ('running', 'succeeded', 'failed', 'timed_out', 'cancelled', 'lease_expired'));

CREATE INDEX steps_workflow_status_idx ON steps (workflow_id, status);
CREATE INDEX step_dependencies_depends_on_idx ON step_dependencies (depends_on_step_id, step_id);

INSERT INTO schema_migrations(version) VALUES (3);
COMMIT;
