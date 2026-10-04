package migrations

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/jackc/pgx/v5/pgxpool"
)

func Run(ctx context.Context, pool *pgxpool.Pool) error {
	var exists bool
	err := pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT FROM information_schema.tables
			WHERE table_schema = 'public' AND table_name = 'schema_migrations'
		)`).Scan(&exists)
	if err != nil {
		return fmt.Errorf("inspect migrations: %w", err)
	}
	var version int64
	if exists {
		err = pool.QueryRow(ctx, `SELECT COALESCE(MAX(version), 0) FROM schema_migrations`).Scan(&version)
		if err != nil {
			return fmt.Errorf("read migration version: %w", err)
		}
	}
	files := []struct {
		version int64
		name    string
	}{
		{1, "001_initial.sql"},
		{2, "002_leases_and_idempotency.sql"},
		{3, "003_workflow_semantics.sql"},
		{4, "004_observability_indexes.sql"},
	}
	for _, migration := range files {
		if migration.version <= version {
			continue
		}
		sql, readErr := readMigration(migration.name)
		if readErr != nil {
			return fmt.Errorf("read migration %03d: %w", migration.version, readErr)
		}
		if _, execErr := pool.Exec(ctx, string(sql)); execErr != nil {
			return fmt.Errorf("apply migration %03d: %w", migration.version, execErr)
		}
	}
	return nil
}

func readMigration(name string) ([]byte, error) {
	directories := []string{os.Getenv("RELAYFLOW_MIGRATIONS_DIR"), "db/migrations", "../../db/migrations"}
	var lastErr error
	for _, directory := range directories {
		if directory == "" {
			continue
		}
		content, err := os.ReadFile(filepath.Join(directory, name))
		if err == nil {
			return content, nil
		}
		lastErr = err
	}
	return nil, lastErr
}
