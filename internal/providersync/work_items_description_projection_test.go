package providersync

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/storedversion"
	"github.com/google/uuid"
)

const pushedWorkItemDescription = "pushed body"

func descriptionTestRows(now time.Time, orgID string, description *string) (githubWorkItemRow, linearWorkItemRow) {
	repoID := uuid.MustParse("c7198fbc-1945-3717-05d8-eb78866b4e79")
	priority := "p1"
	github := githubWorkItemRow{
		WorkItemID: "gl:acme/api#42", Provider: "gitlab", Title: "Described", Type: "issue", Status: "doing",
		Description: description, PriorityRaw: &priority, RepoID: &repoID,
		Assignees: []string{}, Labels: []string{}, CreatedAt: now, UpdatedAt: now, OrgID: orgID, LastSynced: now,
	}
	linear := linearWorkItemRow{
		WorkItemID: "linear:ENG-9", Provider: "linear", Title: "Described", Type: "task", Status: "todo",
		Description: description, PriorityRaw: &priority,
		Assignees: []string{}, Labels: []string{}, CreatedAt: now, UpdatedAt: now, OrgID: orgID, LastSynced: now,
	}
	return github, linear
}

func workItemsEffectOf(t *testing.T, row any) EffectBatch {
	t.Helper()
	encoded, err := json.Marshal(row)
	if err != nil {
		t.Fatal(err)
	}
	effect, err := BuildEffectBatch("work_items", EffectReadbackRequired, []json.RawMessage{encoded})
	if err != nil {
		t.Fatal(err)
	}
	return effect
}

func projectedWorkItemsObject(t *testing.T, provider string, effect EffectBatch) map[string]json.RawMessage {
	t.Helper()
	projected, err := projectPreparedRouteEffects(provider, []EffectBatch{effect})
	if err != nil {
		t.Fatal(err)
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(projected[0].Rows[0], &object); err != nil {
		t.Fatal(err)
	}
	return object
}

// The commit projection of every work-items route keeps the description the
// provider stated and still drops the keys no work_items sink writes.
func TestPreparedRouteProjectionKeepsTheWorkItemsDescription(t *testing.T) {
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	text := "blocks #7 and see <b>CHAOS-9</b> & more"
	github, linear := descriptionTestRows(now, "org-acme", &text)
	for _, tc := range []struct {
		provider string
		row      any
	}{{"github", github}, {"gitlab", github}, {"jira", github}, {"linear", linear}} {
		keys, ok := preparedRouteColumns(tc.provider, "work_items")
		if !ok || !keys["description"] {
			t.Errorf("%s: work_items projection keys=%v, want description kept", tc.provider, keys)
		}
		object := projectedWorkItemsObject(t, tc.provider, workItemsEffectOf(t, tc.row))
		var got string
		if err := json.Unmarshal(object["description"], &got); err != nil || got != text {
			t.Errorf("%s: projected description=%s err=%v, want %q", tc.provider, object["description"], err, text)
		}
		if _, dropped := object["priority_raw"]; dropped {
			t.Errorf("%s: projection kept priority_raw, which no work_items sink writes", tc.provider)
		}
	}
}

// A description is cut to the bound the sink applies before the row reaches
// the effect ledger and the snapshot; a short one, a null and a missing key
// pass through as given.
func TestPreparedRouteProjectionCutsALongDescriptionToTheSinkBound(t *testing.T) {
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	long := strings.Repeat("é", workItemDescriptionMaxRunes+300)
	short := strings.Repeat("<ü>", 10)
	exact := strings.Repeat("x", workItemDescriptionMaxRunes)
	for name, tc := range map[string]struct {
		description *string
		want        string
		wantKey     bool
	}{
		"long":     {&long, *boundedWorkItemDescription(&long), true},
		"short":    {&short, short, true},
		"at bound": {&exact, exact, true},
		"null":     {nil, "", true},
	} {
		github, _ := descriptionTestRows(now, "org-acme", tc.description)
		effect := workItemsEffectOf(t, github)
		projected, err := projectPreparedRouteEffects("gitlab", []EffectBatch{effect})
		if err != nil {
			t.Fatal(err)
		}
		var object map[string]json.RawMessage
		if err := json.Unmarshal(projected[0].Rows[0], &object); err != nil {
			t.Fatal(err)
		}
		raw, present := object["description"]
		if present != tc.wantKey {
			t.Fatalf("%s: description key present=%v", name, present)
		}
		var got *string
		if err := json.Unmarshal(raw, &got); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		switch {
		case tc.description == nil && got != nil:
			t.Errorf("%s: projected %q for a null description", name, *got)
		case tc.description != nil && (got == nil || *got != tc.want):
			t.Errorf("%s: projected description of %d runes, want %d", name, utf8.RuneCountInString(descriptionText(got)), utf8.RuneCountInString(tc.want))
		}
		if projected[0].PayloadBytes > effect.PayloadBytes {
			t.Errorf("%s: projection grew the effect from %d to %d bytes", name, effect.PayloadBytes, projected[0].PayloadBytes)
		}
	}
	// A normalizer row missing the key (an earlier binary's snapshot) gets none.
	object := map[string]json.RawMessage{}
	github, _ := descriptionTestRows(now, "org-acme", &long)
	encoded, _ := json.Marshal(github)
	if err := json.Unmarshal(encoded, &object); err != nil {
		t.Fatal(err)
	}
	delete(object, "description")
	if without := projectedWorkItemsObject(t, "gitlab", workItemsEffectOf(t, object)); without["description"] != nil {
		t.Errorf("a row without a description key got %s", without["description"])
	}
}

func descriptionText(text *string) string {
	if text == nil {
		return ""
	}
	return *text
}

// The snapshot holds the cut description, refuses a row that still carries the
// provider's full text, and does not read a description's text as a key: the
// sensitive-key scan covers keys only.
func TestPreparedRouteSnapshotHoldsTheCutDescription(t *testing.T) {
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	claim, _ := preparedWorkItemsSession(t, now, "github", "work-items")
	long := "reset the password token " + strings.Repeat("é", workItemDescriptionMaxRunes+300)
	github, _ := descriptionTestRows(now, claim.OrgID, &long)
	github.Provider = "github"
	batch := preparedGitHubWorkItemsFixture(t, claim)
	for index, effect := range batch.Effects {
		if effect.Destination == "work_items" {
			batch.Effects[index] = workItemsEffectOf(t, github)
		}
	}
	if _, _, err := encodePreparedRouteManifest(claim, batch, ShadowComparison{Match: true}, now); err == nil {
		t.Fatal("a snapshot took a row that still holds the provider's full description")
	}
	projected, err := projectPreparedRouteEffects(claim.Provider, batch.Effects)
	if err != nil {
		t.Fatal(err)
	}
	batch.Effects = projected
	payload, _, err := encodePreparedRouteManifest(claim, batch, ShadowComparison{Match: true}, now)
	if err != nil {
		t.Fatalf("a projected description with sensitive-looking text was refused: %v", err)
	}
	if strings.Contains(string(payload), strings.Repeat("é", workItemDescriptionMaxRunes+1)) {
		t.Fatal("the snapshot holds more of the description than the sink writes")
	}
	if !strings.Contains(string(payload), "reset the password token") {
		t.Fatal("the snapshot lost the description")
	}
}

// The route as registered: the first attempt commits the projected rows, so a
// description reaches the sink and the stored snapshot.
func TestPreparedWorkItemsRouteCommitsTheDescriptionToTheSinkAndSnapshot(t *testing.T) {
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	claim, session := preparedWorkItemsSession(t, now, "github", "work-items")
	text := "the provider description"
	github, _ := descriptionTestRows(now, claim.OrgID, &text)
	github.Provider = "github"
	batch := preparedGitHubWorkItemsFixture(t, claim)
	for index, effect := range batch.Effects {
		if effect.Destination == "work_items" {
			batch.Effects[index] = workItemsEffectOf(t, github)
		}
	}
	descriptor, ok := Descriptor("github", "work-items")
	if !ok || !descriptor.PreparedManifestRecovery {
		t.Fatalf("github/work-items descriptor=%+v ok=%v", descriptor, ok)
	}
	descriptor.Destinations = githubWorkItemRouteDestinations()
	descriptor.RouteReady, descriptor.Plannable = true, true
	ledger := &memoryEffectLedger{}
	sink := &projectedRowSink{}
	if _, err := preparedGitHubExecutor(now, &staticCompleteRouteHandler{batch: batch}, ledger, sink).
		Execute(context.Background(), session, descriptor); err != nil {
		t.Fatal(err)
	}
	written := sink.rows["work_items"]
	if len(written) != 1 || !strings.Contains(string(written[0]), `"description":"`+text+`"`) {
		t.Fatalf("sink work_items rows=%s, want the description kept", written)
	}
	if strings.Contains(string(written[0]), "priority_raw") {
		t.Fatalf("sink work_items row=%s, want the Python-omitted keys dropped", written[0])
	}
	if !strings.Contains(string(ledger.preparedSnapshot), text) {
		t.Fatal("the stored snapshot lost the description")
	}
}

// A prepared snapshot written before the projection kept description has
// rows with no description key. Replaying one must keep the held description
// (a push-written one) and a row that states one replaces it; a stated empty
// text or null states none, so the held one stays.
func TestReplayOfAPreparedSnapshotWithoutDescriptionKeepsTheHeldDescription(t *testing.T) {
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	claim := nativeTestClaim("github", "work-items")
	stated := "the provider description"
	empty := ""
	for name, tc := range map[string]struct {
		description *string
		dropKey     bool
		want        string
	}{
		"no key (old snapshot)": {nil, true, pushedWorkItemDescription},
		"null":                  {nil, false, pushedWorkItemDescription},
		"empty text":            {&empty, false, pushedWorkItemDescription},
		"stated":                {&stated, false, stated},
	} {
		github, linear := descriptionTestRows(now, claim.OrgID, tc.description)
		writers := map[string]struct {
			effect EffectBatch
			insert string
			id     string
			repo   uuid.UUID
			write  func(conn driver.Conn, effect EffectBatch) error
		}{
			"gitlab": {
				effect: effectWithoutKey(t, github, "description", tc.dropKey), insert: gitHubWorkItemsInsert,
				id: github.WorkItemID, repo: *github.RepoID,
				write: func(conn driver.Conn, effect EffectBatch) error {
					identity := GitHubWorkItemEffectIdentity{
						OrgID: claim.OrgID, Provider: "github", Dataset: "work-items", Generation: claim.GenerationKey(),
						Destination: "work_items", ContentDigest: effect.ContentDigest, RowCount: len(effect.Rows),
					}
					return GitHubWorkItemsClickHouseAdapter{Conn: conn}.WriteGitHubWorkItemEffect(context.Background(), identity, effect)
				},
			},
			"linear": {
				effect: effectWithoutKey(t, linear, "description", tc.dropKey), insert: linearWorkItemsInsert,
				id: linear.WorkItemID,
				write: func(conn driver.Conn, effect EffectBatch) error {
					identity, err := newLinearWorkItemEffectIdentity(nativeTestClaim("linear", "work-items"), effect)
					if err != nil {
						return err
					}
					return LinearWorkItemsClickHouseAdapter{Conn: conn}.WriteLinearWorkItemEffect(context.Background(), identity, effect)
				},
			},
		}
		for provider, writer := range writers {
			conn := &heldVersionConn{held: map[string]*time.Time{writer.id: nil}, repos: map[string]uuid.UUID{writer.id: writer.repo}}
			if err := writer.write(conn, writer.effect); err != nil {
				t.Fatalf("%s/%s: %v", name, provider, err)
			}
			positions, err := storedversion.Positions(withHeldWorkItemColumns(writer.insert))
			if err != nil || len(conn.appended) != 1 {
				t.Fatalf("%s/%s: err=%v appended=%d", name, provider, err, len(conn.appended))
			}
			var got string
			switch description := conn.appended[0][positions["description"]].(type) {
			case string:
				got = description
			case *string:
				got = descriptionText(description)
			}
			if got != tc.want {
				t.Errorf("%s/%s: stored description %q, want %q", name, provider, got, tc.want)
			}
		}
	}
}

func effectWithoutKey(t *testing.T, row any, key string, dropKey bool) EffectBatch {
	t.Helper()
	encoded, err := json.Marshal(row)
	if err != nil {
		t.Fatal(err)
	}
	if dropKey {
		var object map[string]json.RawMessage
		if err := json.Unmarshal(encoded, &object); err != nil {
			t.Fatal(err)
		}
		if _, present := object[key]; !present {
			t.Fatalf("a normalizer row must carry the %s key", key)
		}
		delete(object, key)
		if encoded, err = json.Marshal(object); err != nil {
			t.Fatal(err)
		}
	}
	effect, err := BuildEffectBatch("work_items", EffectReadbackRequired, []json.RawMessage{encoded})
	if err != nil {
		t.Fatal(err)
	}
	return effect
}
