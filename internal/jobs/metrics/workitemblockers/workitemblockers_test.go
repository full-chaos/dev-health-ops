package workitemblockers

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workitemmetrics"
)

// The end lookup is a function of the relations alone: plain ids and
// external keys apart, sorted, no repeat, no empty key.
func TestEndLookupSplitsIdsFromExternalKeys(t *testing.T) {
	relations := []workitemmetrics.BlockingRelation{
		{SourceID: "jira:OPS-1", TargetID: "jira:OPS-2"},
		{SourceID: "gh:acme/api#7", TargetID: "extkey: ops-9 "},
		{SourceID: "gh:acme/api#7", TargetID: "extkey:OPS-9"},
		{SourceID: "gh:acme/api#8", TargetID: "extkey:"},
		{SourceID: "jira:OPS-2", TargetID: "jira:OPS-1"},
	}
	ids, keys := endLookup(relations)
	wantIDs := []string{"gh:acme/api#7", "gh:acme/api#8", "jira:OPS-1", "jira:OPS-2"}
	wantKeys := []string{"OPS-9"}
	if !reflect.DeepEqual(ids, wantIDs) || !reflect.DeepEqual(keys, wantKeys) {
		t.Fatalf("lookup = %v, %v; want %v, %v", ids, keys, wantIDs, wantKeys)
	}
	if ids, keys := endLookup(nil); ids != nil || keys != nil {
		t.Fatalf("no relation: lookup = %v, %v, want nothing", ids, keys)
	}
}

// A read is never run unscoped: no connection or no organization is refused
// before any statement is issued.
func TestReadsRefuseAMissingConnectionOrOrganization(t *testing.T) {
	ctx := context.Background()
	if _, err := LoadRelations(ctx, nil, "org"); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("LoadRelations with no connection: %v", err)
	}
	if _, err := LoadEnds(ctx, nil, "org", nil); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("LoadEnds with no connection: %v", err)
	}
	if _, err := LoadBlockedIntervals(ctx, nil, "org"); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("LoadBlockedIntervals with no connection: %v", err)
	}
	if _, err := LoadRelationsNaming(ctx, nil, "org", []string{"jira:OPS-1"}, 10); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("LoadRelationsNaming with no connection: %v", err)
	}
	if _, err := LoadEndsLimited(ctx, nil, "org", nil, 10); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("LoadEndsLimited with no connection: %v", err)
	}
}
