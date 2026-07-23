package joboutbox

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

func TestDeferredProducerPolicyIsNarrowAndNonExecutable(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		descriptor jobruntime.Descriptor
		deferred   bool
		want       bool
	}{
		{
			name: "reviewed celery handoff",
			descriptor: jobruntime.Descriptor{
				MigrationState: "go_implemented", Route: "celery", RollbackRoute: "celery",
			},
			deferred: true,
			want:     true,
		},
		{
			name: "unreviewed celery handoff",
			descriptor: jobruntime.Descriptor{
				MigrationState: "contract_frozen", Route: "celery", RollbackRoute: "celery",
			},
			deferred: true,
		},
		{
			name: "active route through deferred API",
			descriptor: jobruntime.Descriptor{
				MigrationState: "go_default", Route: "river", RollbackRoute: "celery",
			},
			deferred: true,
		},
		{
			name: "active route through normal API",
			descriptor: jobruntime.Descriptor{
				MigrationState: "go_default", Route: "river", RollbackRoute: "celery",
			},
			want: true,
		},
		{
			name: "celery route through normal API",
			descriptor: jobruntime.Descriptor{
				MigrationState: "go_implemented", Route: "celery", RollbackRoute: "celery",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := descriptorAllowsPublish(test.descriptor, test.deferred); got != test.want {
				t.Fatalf("descriptorAllowsPublish() = %t, want %t", got, test.want)
			}
		})
	}
}
