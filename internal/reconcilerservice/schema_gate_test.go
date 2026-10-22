package reconcilerservice

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/pgmigrate/schemagate/schemagatetest"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
)

type recordedReconcilerDatabase struct {
	*fakeReconcilerDatabase
	schemagatetest.Database
}

func buildForSchemaGate(t *testing.T, database schemagatetest.Database, fake *fakeReconcilerDatabase) *reconcilerDependencies {
	t.Helper()
	return buildReconcilerDependencies(
		context.Background(), config.Config{RiverDatabaseSchema: "river"}, health.NewRegistry(time.Second),
		reconcilerTestLogger(), checkedInReconcilerActivation,
		reconcilerSourcesForTest(t, recordedReconcilerDatabase{fakeReconcilerDatabase: fake, Database: database}),
	)
}

func TestReconcilerRefusesToRunOnADatabaseBehindTheBuild(t *testing.T) {
	fake := &fakeReconcilerDatabase{}
	dependencies := buildForSchemaGate(t, schemagatetest.OneBehind(t), fake)
	var coded interface{ DependencyReason() string }
	if !errors.As(dependencies.databaseErr, &coded) || coded.DependencyReason() != "reconciler_schema_behind" {
		t.Fatalf("a database one migration behind: error = %v, want reconciler_schema_behind", dependencies.databaseErr)
	}
	if dependencies.database != nil || dependencies.loop != nil {
		t.Fatal("the reconciler kept a database or a loop on a schema behind the build")
	}
	if !fake.closed.Load() {
		t.Fatal("the refused database was not closed")
	}
}

func TestReconcilerKeepsADatabaseAtOrAheadOfTheBuild(t *testing.T) {
	for name, database := range map[string]schemagatetest.Database{
		"equal": schemagatetest.AtHead(t),
		"ahead": schemagatetest.Ahead(t),
	} {
		dependencies := buildForSchemaGate(t, database, &fakeReconcilerDatabase{})
		var coded interface{ DependencyReason() string }
		if errors.As(dependencies.databaseErr, &coded) && coded.DependencyReason() == "reconciler_schema_behind" {
			t.Fatalf("%s: the gate refused a database that is not behind", name)
		}
		dependencies.close()
	}
}
