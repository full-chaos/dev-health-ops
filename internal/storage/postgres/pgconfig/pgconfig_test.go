package pgconfig

import (
	"reflect"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/queuecontrol"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

func TestRuntimeConfigMapsEveryPlatformField(t *testing.T) {
	cfg := config.Config{
		DomainDatabaseURI:           secrets.NewSecret("DOMAIN", "postgres://d"),
		QueueDatabaseURI:            secrets.NewSecret("QUEUE", "postgres://q"),
		CoordinatorDatabaseURI:      secrets.NewSecret("COORD", "postgres://c"),
		DomainDatabaseRole:          "domain_role",
		QueueDatabaseRole:           "queue_role",
		CoordinatorDatabaseRole:     "coord_role",
		RiverDatabaseSchema:         "river_x",
		QueueDatabaseMode:           queuecontrol.Session,
		CoordinatorDatabaseMode:     queuecontrol.Transaction,
		DomainTransactionPooler:     true,
		DomainDatabaseMaxConns:      7,
		QueueDatabaseMaxConns:       3,
		CoordinatorDatabaseMaxConns: 2,
	}
	got := RuntimeConfig(cfg)

	if got.DomainURI != "postgres://d" || got.QueueControlURI != "postgres://q" || got.CoordinatorURI != "postgres://c" {
		t.Fatalf("URIs not mapped: %+v", got)
	}
	if got.DomainRole != "domain_role" || got.QueueRole != "queue_role" || got.CoordinatorRole != "coord_role" || got.RiverSchema != "river_x" {
		t.Fatalf("roles or schema not mapped: %+v", got)
	}
	if got.QueueControlMode != queuecontrol.Session || got.CoordinatorMode != queuecontrol.Transaction {
		t.Fatalf("modes not mapped: %+v", got)
	}
	if !got.DomainTransactionPooler || got.DomainMaxConns != 7 || got.QueueMaxConns != 3 || got.CoordinatorMaxConns != 2 {
		t.Fatalf("pool bounds not mapped: %+v", got)
	}
	if got.RequireCoordinator {
		t.Fatal("the adapter must not opt a process into the coordinator")
	}
	// Every field the adapter does not set on purpose must be listed here so a new
	// RuntimeConfig field cannot be silently left unmapped.
	typ := reflect.TypeOf(got)
	val := reflect.ValueOf(got)
	for i := 0; i < typ.NumField(); i++ {
		if typ.Field(i).Name == "RequireCoordinator" {
			continue
		}
		if val.Field(i).IsZero() {
			t.Errorf("RuntimeConfig.%s is zero after mapping a fully populated Config", typ.Field(i).Name)
		}
	}
}
