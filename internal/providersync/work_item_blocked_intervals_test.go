package providersync

// CHAOS-8493 at the sync-time deriver: a work item is "blocked" while a
// blocking relation names an open blocker, for every provider that carries
// such a relation.
//
// Each provider case goes through that provider's REAL relation normalizer,
// fed the payload shape the provider returns, so a normalizer that stopped
// emitting the canonical "blocks" row (or turned it around) fails here by the
// provider's name. The rule itself is pinned clause by clause in
// internal/jobs/metrics/workitemmetrics; these tests pin that each provider's
// rows REACH it, and reach it with the direction the rule reads.

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workitemmetrics"
)

// blockedFactsSource is a derivation source whose store holds the given
// blocking facts. It records what the loader asked it for.
type blockedFactsSource struct {
	relations []workitemmetrics.BlockingRelation
	ends      []workitemmetrics.RelationEnd
	err       error

	calls   int
	itemIDs []string
	fresh   []workitemmetrics.BlockingRelation
}

var (
	blockedTestDay        = time.Date(2026, 8, 4, 0, 0, 0, 0, time.UTC)
	blockedTestSyncedAt   = time.Date(2026, 8, 6, 0, 0, 0, 0, time.UTC)
	blockedTestComputedAt = time.Date(2026, 8, 6, 0, 0, 0, 0, time.UTC)
)

func blockedTestClaim(t *testing.T, provider string) Claim {
	t.Helper()
	claim := githubWorkItemOracleClaim()
	claim.Provider = provider
	capability, ok := Capability(provider, "work-items")
	if !ok {
		t.Fatalf("no work-items capability for %s", provider)
	}
	claim.CostClass = capability.CostClass
	if err := claim.Validate(); err != nil {
		t.Fatalf("%s claim: %v", provider, err)
	}
	return claim
}

// The gitlab description keyword "blocks" (CHAOS-8493's named case 1, exact
// since CHAOS-8578).
//
// "blocks #7" in the description of #5 is stored with the raw value "blocks",
// which is also the raw value of a native issue link seen from the blocker. A
// native link is written by both issues; the keyword only by #5. The
// normalizer now stores the writer (relation_writer): "source" for the
// keyword, "both" for a native link. So a later sync of the BLOCKED issue #7,
// which holds no text about #5, ends nothing.
//
// A row with no writer stored (written before migration 103 and not since)
// keeps the old behaviour: both issues are taken as writers, and the
// relation ends at its last write when #7 is synced later.
func TestAGitLabBlocksKeywordEndsOnlyWhenItsDescriptionIsSyncedWithoutIt(t *testing.T) {
	claim := blockedTestClaim(t, "gitlab")
	written := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	firstSeen := written.AddDate(0, 0, -2)
	later := written.Add(24 * time.Hour)
	ends := []workitemmetrics.RelationEnd{
		{WorkItemID: "gitlab:acme/api#5", Provider: "gitlab", Status: "in_progress", CreatedAt: written.AddDate(0, 0, -9), LastSynced: written},
		// The blocked issue was synced again; it holds no text about #5.
		{WorkItemID: "gitlab:acme/api#7", Provider: "gitlab", Status: "in_progress", CreatedAt: written.AddDate(0, 0, -5), LastSynced: later},
	}
	open := workitemmetrics.BlockedInterval{Start: firstSeen}
	for name, tc := range map[string]struct {
		description  string
		raw          string
		writerStored bool
		want         workitemmetrics.BlockedInterval
	}{
		"blocks, writer stored":      {"this blocks #7", "explicit_text:blocks", true, open},
		"blocks, no writer stored":   {"this blocks #7", "explicit_text:blocks", false, workitemmetrics.BlockedInterval{Start: firstSeen, End: &written}},
		"blocking, writer stored":    {"this is blocking #7", "explicit_text:blocking", true, open},
		"blocking, no writer stored": {"this is blocking #7", "explicit_text:blocking", false, open},
	} {
		t.Run(name, func(t *testing.T) {
			rows := normalizeGitLabDependencies(claim, "gitlab:acme/api#5", "acme/api", tc.description, nil, written)
			if len(rows) != 1 || rows[0].SourceWorkItemID != "gitlab:acme/api#5" || rows[0].TargetWorkItemID != "gitlab:acme/api#7" ||
				rows[0].RelationshipType != "blocks" || rows[0].RelationshipTypeRaw != tc.raw {
				t.Fatalf("the description %q gave %+v, want one `blocks` row from #5 to #7 with the raw value %q", tc.description, rows, tc.raw)
			}
			if rows[0].RelationWriter == nil || *rows[0].RelationWriter != workitemmetrics.RelationWriterSource {
				t.Fatalf("the keyword row stores the writer %v, want %q (the description holds it)", rows[0].RelationWriter, workitemmetrics.RelationWriterSource)
			}
			relation := workitemmetrics.BlockingRelation{
				SourceID: rows[0].SourceWorkItemID, TargetID: rows[0].TargetWorkItemID, RelationshipType: rows[0].RelationshipType,
				Raw: rows[0].RelationshipTypeRaw, SemanticsVersion: rows[0].RelationshipSemanticsVersion,
				LastSynced: rows[0].LastSynced, FirstSeenAt: &firstSeen,
			}
			if tc.writerStored {
				relation.Writer = rows[0].RelationWriter
			}
			got := workitemmetrics.BlockedIntervalsByItem([]workitemmetrics.BlockingRelation{relation}, ends)
			if want := (map[string][]workitemmetrics.BlockedInterval{"gitlab:acme/api#7": {tc.want}}); !reflect.DeepEqual(got, want) {
				t.Fatalf("intervals = %+v, want %+v", got, want)
			}
		})
	}
	// The native link from the blocker's side stores the same raw value.
	var links []gitlabIssueLinkPayload
	if err := json.Unmarshal([]byte(`[{"link_type":"blocks","iid":7,"references":{"full":"acme/api#7"},"link_created_at":"2026-07-30T09:15:00.000Z"}]`), &links); err != nil {
		t.Fatal(err)
	}
	native := normalizeGitLabDependencies(claim, "gitlab:acme/api#5", "acme/api", "", links, written)
	if len(native) != 1 || native[0].RelationshipTypeRaw != "blocks" || native[0].SourceWorkItemID != "gitlab:acme/api#5" {
		t.Fatalf("the native link gave %+v, want one row with the raw value `blocks`", native)
	}
	// The native link is written by both issues, and starts at gitlab's own
	// time of the link.
	// Between the issues' creation and the first sync that saw the link: the
	// provider's time is the start, not first seen.
	linked := time.Date(2026, 7, 30, 9, 15, 0, 0, time.UTC)
	if native[0].RelationWriter == nil || *native[0].RelationWriter != workitemmetrics.RelationWriterBoth ||
		native[0].RelationStartedAt == nil || !native[0].RelationStartedAt.Equal(linked) {
		t.Fatalf("the native link stores writer %v and start %v, want %q and %s", native[0].RelationWriter, native[0].RelationStartedAt, workitemmetrics.RelationWriterBoth, linked)
	}
	nativeRelation := workitemmetrics.BlockingRelation{
		SourceID: native[0].SourceWorkItemID, TargetID: native[0].TargetWorkItemID, RelationshipType: native[0].RelationshipType,
		Raw: native[0].RelationshipTypeRaw, SemanticsVersion: native[0].RelationshipSemanticsVersion,
		LastSynced: native[0].LastSynced, StartedAt: native[0].RelationStartedAt, FirstSeenAt: &firstSeen, Writer: native[0].RelationWriter,
	}
	// A later sync of either issue that did not write the link ends it.
	got := workitemmetrics.BlockedIntervalsByItem([]workitemmetrics.BlockingRelation{nativeRelation}, ends)
	if want := (map[string][]workitemmetrics.BlockedInterval{"gitlab:acme/api#7": {{Start: linked, End: &written}}}); !reflect.DeepEqual(got, want) {
		t.Fatalf("native link: intervals = %+v, want %+v", got, want)
	}
}

// CHAOS-8578: the linear and gitlab normalizers store the provider's own time
// of a native link as the relation's start; a payload with no time, or one
// that does not parse, stores none (the rule then takes first seen). Only
// gitlab stores the writer.
func TestNativeLinksStoreTheProvidersOwnLinkTime(t *testing.T) {
	linked := time.Date(2026, 7, 30, 9, 15, 0, 0, time.UTC)
	for _, tc := range []struct{ name, createdAt string }{
		{"with a time", `,"createdAt":"2026-07-30T09:15:00.000Z"`},
		{"with no time", ``},
		{"with a time that does not parse", `,"createdAt":"yesterday"`},
	} {
		t.Run("linear "+tc.name, func(t *testing.T) {
			claim := blockedTestClaim(t, "linear")
			var payload linearWorkItemPayload
			if err := json.Unmarshal([]byte(`{"identifier":"OPS-2","inverseRelations":{"nodes":[
				{"type":"blocks"`+tc.createdAt+`,"issue":{"identifier":"OPS-1"},"relatedIssue":{"identifier":"OPS-2"}}
			]}}`), &payload); err != nil {
				t.Fatal(err)
			}
			rows := normalizeLinearDependencies(claim, payload, "linear:OPS-2", blockedTestSyncedAt)
			if len(rows) != 1 || rows[0].RelationWriter != nil {
				t.Fatalf("rows = %+v, want one row with no stored writer", rows)
			}
			switch {
			case tc.name == "with a time" && (rows[0].RelationStartedAt == nil || !rows[0].RelationStartedAt.Equal(linked)):
				t.Fatalf("start = %v, want %s", rows[0].RelationStartedAt, linked)
			case tc.name != "with a time" && rows[0].RelationStartedAt != nil:
				t.Fatalf("start = %v, want none", *rows[0].RelationStartedAt)
			}
		})
	}
	t.Run("gitlab with no time", func(t *testing.T) {
		claim := blockedTestClaim(t, "gitlab")
		var links []gitlabIssueLinkPayload
		if err := json.Unmarshal([]byte(`[{"link_type":"is_blocked_by","iid":5,"references":{"full":"acme/api#5"}}]`), &links); err != nil {
			t.Fatal(err)
		}
		rows := normalizeGitLabDependencies(claim, "gitlab:acme/api#7", "acme/api", "", links, blockedTestSyncedAt)
		if len(rows) != 1 || rows[0].RelationStartedAt != nil || rows[0].RelationWriter == nil || *rows[0].RelationWriter != workitemmetrics.RelationWriterBoth {
			t.Fatalf("rows = %+v, want one row with no start and the writer %q", rows, workitemmetrics.RelationWriterBoth)
		}
		// A "blocked by" keyword in #7's description: the row is turned
		// around (#9 blocks #7), and its writer is the target, #7.
		keyword := normalizeGitLabDependencies(claim, "gitlab:acme/api#7", "acme/api", "blocked by #9", nil, blockedTestSyncedAt)
		if len(keyword) != 1 || keyword[0].SourceWorkItemID != "gitlab:acme/api#9" || keyword[0].TargetWorkItemID != "gitlab:acme/api#7" ||
			keyword[0].RelationWriter == nil || *keyword[0].RelationWriter != workitemmetrics.RelationWriterTarget {
			t.Fatalf("keyword rows = %+v, want #9 -> #7 written by the target", keyword)
		}
	})
}
