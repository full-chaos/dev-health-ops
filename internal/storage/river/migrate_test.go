package riverstore

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"
)

func TestPinnedMigrationBundleIsExactPrefix(t *testing.T) {
	t.Parallel()

	migrator, err := rivermigrate.New(riverpgxv5.New(nil), nil)
	if err != nil {
		t.Fatal(err)
	}
	versions := migrator.AllVersions()
	if err := validatePinnedBundle(versions); err != nil {
		t.Fatal(err)
	}
	if versions[len(versions)-1].Version != PinnedSchemaVersion {
		t.Fatalf("latest bundled version = %d, want %d", versions[len(versions)-1].Version, PinnedSchemaVersion)
	}
}

func TestPinnedMigratorRequiresTwoConnectionsForLockAndCommitSeparatedMigrations(t *testing.T) {
	t.Parallel()

	poolConfig, err := pgxpool.ParseConfig("postgres://migration@127.0.0.1:1/app?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	poolConfig.MaxConns = 1
	pool, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	_, err = ApplyPinnedMigrations(context.Background(), pool, MigrationOptions{
		Schema:     "river",
		DomainRole: "domain_runtime",
		QueueRole:  "queue_runtime",
	})
	if !errors.Is(err, ErrMigrationConfiguration) {
		t.Fatalf("ApplyPinnedMigrations() error = %v", err)
	}
}

func TestLongRunningCommandsCannotAutoMigrate(t *testing.T) {
	t.Parallel()

	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate test source")
	}
	repositoryRoot := filepath.Clean(filepath.Join(filepath.Dir(currentFile), "..", "..", ".."))
	for _, command := range []string{
		"dev-health-worker",
		"dev-health-scheduler",
		"dev-health-reconciler",
		"dev-health-stream-runner",
	} {
		directory := filepath.Join(repositoryRoot, "cmd", command)
		entries, err := os.ReadDir(directory)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
				continue
			}
			path := filepath.Join(directory, entry.Name())
			contents, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			text := string(contents)
			for _, forbidden := range []string{"rivermigrate", "ApplyPinnedMigrations", "dev-health-worker-migrate"} {
				if strings.Contains(text, forbidden) {
					t.Fatalf("long-running command %s file %s references migration surface %q", command, entry.Name(), forbidden)
				}
			}
		}
	}
}

func TestPinnedMigrationBundleRejectsPrefixOrSuffixDrift(t *testing.T) {
	t.Parallel()

	tests := [][]rivermigrate.Migration{
		{{Version: 1}},
		{{Version: 1}, {Version: 3}, {Version: 2}, {Version: 4}, {Version: 5}, {Version: 6}, {Version: 7}},
		{{Version: 1}, {Version: 2}, {Version: 3}, {Version: 4}, {Version: 5}, {Version: 6}, {Version: 7}, {Version: 8}},
	}
	for _, versions := range tests {
		if err := validatePinnedBundle(versions); !errors.Is(err, ErrPinnedMigrationMismatch) {
			t.Fatalf("validatePinnedBundle() error = %v", err)
		}
	}
}

func TestMigrationOptionsRequireSeparateSafeIdentifiers(t *testing.T) {
	t.Parallel()

	valid := MigrationOptions{Schema: "river", DomainRole: "dev_health_domain", QueueRole: "dev_health_queue"}
	if err := ValidateMigrationOptions(valid); err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []MigrationOptions{
		{},
		{Schema: "River-Bad", DomainRole: valid.DomainRole, QueueRole: valid.QueueRole},
		{Schema: valid.Schema, DomainRole: "same", QueueRole: "same"},
		{Schema: valid.Schema, DomainRole: "domain; DROP SCHEMA public", QueueRole: valid.QueueRole},
	} {
		if err := ValidateMigrationOptions(invalid); !errors.Is(err, ErrMigrationConfiguration) {
			t.Fatalf("ValidateMigrationOptions(%#v) error = %v", invalid, err)
		}
	}
}

func TestRuntimeRolePreflightRequiresSeparateLeastPrivilegeLoginRoles(t *testing.T) {
	t.Parallel()

	options := MigrationOptions{
		Schema:     "river",
		DomainRole: "domain_runtime",
		QueueRole:  "queue_runtime",
	}
	tests := []struct {
		name           string
		migrationRole  string
		domainEligible bool
		queueEligible  bool
		wantErr        bool
	}{
		{name: "separate least privilege login roles", migrationRole: "migration_owner", domainEligible: true, queueEligible: true},
		{name: "domain role missing or privileged", migrationRole: "migration_owner", queueEligible: true, wantErr: true},
		{name: "queue role missing or privileged", migrationRole: "migration_owner", domainEligible: true, wantErr: true},
		{name: "migration uses domain role", migrationRole: options.DomainRole, domainEligible: true, queueEligible: true, wantErr: true},
		{name: "migration uses queue role", migrationRole: options.QueueRole, domainEligible: true, queueEligible: true, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateRuntimeRolePreflight(
				test.migrationRole,
				test.domainEligible,
				test.queueEligible,
				options,
			)
			if test.wantErr && !errors.Is(err, ErrMigrationConfiguration) {
				t.Fatalf("validateRuntimeRolePreflight() error = %v", err)
			}
			if !test.wantErr && err != nil {
				t.Fatalf("validateRuntimeRolePreflight() error = %v", err)
			}
		})
	}
}
