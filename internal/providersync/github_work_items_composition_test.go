package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// githubWorkItemEngineTestDestinations is the engine-dependent set exercised by
// the composition doubles below.
var githubWorkItemEngineTestDestinations = []string{
	"issue_type_metrics_daily",
}

type stubWorkItemConn struct{ driver.Conn }

func githubWorkItemCompositionLease() providerfoundation.LeaseGuard {
	return providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
}

// TestNewGitHubWorkItemClickHouseEffectsRejectsMissingDependencies observes each
// constructor guard failing on its own. A constructor that silently accepted a
// nil connection would hand back a sink whose every adapter panics on first
// use, far from the call site that omitted it.
func TestNewGitHubWorkItemClickHouseEffectsRejectsMissingDependencies(t *testing.T) {
	t.Parallel()
	conn := stubWorkItemConn{}
	lease := githubWorkItemCompositionLease()

	for _, testCase := range []struct {
		name  string
		conn  driver.Conn
		lease providerfoundation.LeaseGuard
	}{
		{name: "nil conn", conn: nil, lease: lease},
		{name: "nil lease", conn: conn, lease: nil},
		{name: "both nil", conn: nil, lease: nil},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			sink, err := NewGitHubWorkItemClickHouseEffects(testCase.conn, testCase.lease, nil)
			if !errors.Is(err, ErrInvalidConfiguration) {
				t.Fatalf("error=%v want ErrInvalidConfiguration", err)
			}
			// The zero sink must not be mistakable for a usable one.
			if !reflect.DeepEqual(sink, GitHubWorkItemClickHouseEffects{}) {
				t.Fatalf("rejected build returned a populated sink: %+v", sink)
			}
			if sink.complete() {
				t.Fatal("rejected build reported itself complete")
			}
		})
	}
}

// TestNewGitHubWorkItemClickHouseEffectsWiresAllSixteen is the constructor
// tripwire: every canonical destination must be concrete before the returned
// sink may be used.
func TestNewGitHubWorkItemClickHouseEffectsWiresAllSixteen(t *testing.T) {
	t.Parallel()
	sink, err := NewGitHubWorkItemClickHouseEffects(
		stubWorkItemConn{}, githubWorkItemCompositionLease(), nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	missing := sink.MissingDestinations()
	if len(missing) != 0 {
		t.Fatalf("missing=%v want none", missing)
	}
	if !sink.complete() {
		t.Fatal("fully constructed sink did not report itself complete")
	}
	for _, destination := range githubWorkItemRouteDestinations() {
		adapter, known := sink.adapterForDestination(destination)
		if !known {
			t.Fatalf("destination %q is not dispatchable", destination)
		}
		if adapter == nil {
			t.Errorf("destination %q should be wired but is nil", destination)
		}
	}
	if sink.Lease == nil {
		t.Fatal("composite lease guard was not installed")
	}
}

// TestGitHubWorkItemSinkMissingDestinationsIsClauseLevel nils each of the
// sixteen slots in turn against an otherwise COMPLETE sink. A single mutation of
// the whole completeness check would be killed by any one of these; only the
// per-slot sweep proves no individual slot is unchecked.
func TestGitHubWorkItemSinkMissingDestinationsIsClauseLevel(t *testing.T) {
	t.Parallel()
	claim := nativeTestClaim("github", "work-items")
	effects, err := BuildGitHubWorkItemEffects(GitHubWorkItemEffectRows{})
	if err != nil {
		t.Fatal(err)
	}
	complete := githubWorkItemEffectsFixture(newSemanticWorkItemEffectBackend())
	if got := complete.MissingDestinations(); len(got) != 0 {
		t.Fatalf("fixture is not complete: missing=%v", got)
	}

	for _, destination := range githubWorkItemRouteDestinations() {
		t.Run(destination, func(t *testing.T) {
			t.Parallel()
			mutated := githubWorkItemSinkWithout(
				githubWorkItemEffectsFixture(newSemanticWorkItemEffectBackend()),
				destination,
			)
			if got := mutated.MissingDestinations(); !reflect.DeepEqual(
				got, []string{destination},
			) {
				t.Fatalf("missing=%v want=[%s]", got, destination)
			}
			if mutated.complete() {
				t.Fatalf("sink without %s reported complete", destination)
			}
			// The gate must reject writes to EVERY destination, not only the
			// one that is missing: a partial sink is not partially usable.
			for _, effect := range effects {
				if err := mutated.WriteEffect(
					context.Background(), claim, effect,
				); !errors.Is(err, ErrInvalidConfiguration) {
					t.Fatalf("write %s with %s missing: error=%v",
						effect.Destination, destination, err)
				}
				if _, err := mutated.InspectEffect(
					context.Background(), claim, effect,
				); !errors.Is(err, ErrInvalidConfiguration) {
					t.Fatalf("inspect %s with %s missing: error=%v",
						effect.Destination, destination, err)
				}
			}
		})
	}
}

// githubWorkItemSinkWithout clears exactly one slot by destination name. It
// switches over the same canonical list the production dispatch does, so a
// destination added without a case here fails the test rather than silently
// leaving the sink complete.
func githubWorkItemSinkWithout(
	sink GitHubWorkItemClickHouseEffects,
	destination string,
) GitHubWorkItemClickHouseEffects {
	switch destination {
	case "ai_attribution":
		sink.AIAttribution = nil
	case "sprints":
		sink.Sprints = nil
	case "work_item_dependencies":
		sink.WorkItemDependencies = nil
	case "work_item_interactions":
		sink.WorkItemInteractions = nil
	case "work_item_reopen_events":
		sink.WorkItemReopenEvents = nil
	case "work_item_transitions":
		sink.WorkItemTransitions = nil
	case "work_items":
		sink.WorkItems = nil
	case "project_membership_transitions":
		sink.ProjectMembershipTransitions = nil
	case "projects":
		sink.Projects = nil
	default:
		panic("unmapped destination " + destination)
	}
	return sink
}

// githubWorkItemStubEngine isolates composition mechanics from the real config
// engines so error, omission and multi-day accumulation paths stay focused.
type githubWorkItemStubEngine struct{ err error }

// githubWorkItemSilentEngine returns success and nothing else -- the fail-open
// shape: a producer that answers "fine, no rows" where it should have refused.
type githubWorkItemSilentEngine struct {
	rows map[string][]json.RawMessage
}
