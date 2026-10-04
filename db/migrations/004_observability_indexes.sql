BEGIN;

CREATE INDEX workflow_events_event_type_idx ON workflow_events (event_type);
CREATE INDEX attempts_finished_at_idx ON attempts (finished_at) WHERE finished_at IS NOT NULL;

INSERT INTO schema_migrations(version) VALUES (4);
COMMIT;
