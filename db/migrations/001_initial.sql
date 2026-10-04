BEGIN;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS schema_migrations (
    version bigint PRIMARY KEY,
    applied_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE workflows (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    idempotency_key text NOT NULL UNIQUE CHECK (length(idempotency_key) BETWEEN 1 AND 200),
    name text NOT NULL CHECK (length(name) BETWEEN 1 AND 200),
    status text NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'running', 'succeeded', 'failed', 'cancelled')),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz
);

CREATE TABLE steps (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_id uuid NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
    step_key text NOT NULL CHECK (length(step_key) BETWEEN 1 AND 100),
    kind text NOT NULL CHECK (kind IN ('echo', 'noop')),
    input jsonb NOT NULL DEFAULT '{}'::jsonb,
    output jsonb,
    status text NOT NULL DEFAULT 'ready'
        CHECK (status IN ('blocked', 'ready', 'running', 'succeeded', 'failed', 'cancelled')),
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    available_at timestamptz NOT NULL DEFAULT now(),
    worker_id text,
    lease_token uuid,
    lease_expires_at timestamptz,
    error text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    started_at timestamptz,
    completed_at timestamptz,
    UNIQUE (workflow_id, step_key),
    CHECK ((status = 'running') = (worker_id IS NOT NULL)),
    CHECK ((status = 'running') = (lease_token IS NOT NULL)),
    CHECK ((status = 'running') = (lease_expires_at IS NOT NULL))
);

CREATE TABLE step_dependencies (
    step_id uuid NOT NULL REFERENCES steps(id) ON DELETE CASCADE,
    depends_on_step_id uuid NOT NULL REFERENCES steps(id) ON DELETE CASCADE,
    PRIMARY KEY (step_id, depends_on_step_id),
    CHECK (step_id <> depends_on_step_id)
);

CREATE TABLE attempts (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    step_id uuid NOT NULL REFERENCES steps(id) ON DELETE CASCADE,
    attempt_no integer NOT NULL CHECK (attempt_no > 0),
    worker_id text NOT NULL,
    lease_token uuid NOT NULL,
    status text NOT NULL CHECK (status IN ('running', 'succeeded', 'failed', 'lease_expired')),
    started_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    error text,
    UNIQUE (step_id, attempt_no)
);

CREATE TABLE workflow_events (
    id bigserial PRIMARY KEY,
    workflow_id uuid NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
    step_id uuid REFERENCES steps(id) ON DELETE CASCADE,
    attempt_id uuid REFERENCES attempts(id) ON DELETE SET NULL,
    event_type text NOT NULL,
    payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX steps_ready_claim_idx ON steps (available_at, created_at)
    WHERE status = 'ready';
CREATE INDEX steps_expired_lease_idx ON steps (lease_expires_at)
    WHERE status = 'running';
CREATE INDEX workflow_events_workflow_id_id_idx ON workflow_events (workflow_id, id);
CREATE INDEX attempts_step_id_attempt_no_idx ON attempts (step_id, attempt_no DESC);

CREATE OR REPLACE FUNCTION prevent_event_mutation() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'workflow_events is append-only';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER workflow_events_no_update
BEFORE UPDATE OR DELETE ON workflow_events
FOR EACH ROW EXECUTE FUNCTION prevent_event_mutation();

INSERT INTO schema_migrations(version) VALUES (1);
COMMIT;
