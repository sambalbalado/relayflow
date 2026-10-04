BEGIN;

ALTER TABLE steps DROP CONSTRAINT steps_kind_check;
ALTER TABLE steps ADD CONSTRAINT steps_kind_check CHECK (kind IN ('echo', 'noop', 'sleep'));

ALTER TABLE steps ADD COLUMN operation_key text;
UPDATE steps SET operation_key = workflow_id::text || ':' || step_key;
ALTER TABLE steps ALTER COLUMN operation_key SET NOT NULL;
ALTER TABLE steps ADD CONSTRAINT steps_operation_key_unique UNIQUE (operation_key);

INSERT INTO schema_migrations(version) VALUES (2);
COMMIT;
