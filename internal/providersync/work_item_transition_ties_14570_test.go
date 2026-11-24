package providersync

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"
)

func linearCloseReproPayload14570(t *testing.T, identifier, state, stateType, history string) linearWorkItemPayload {
	t.Helper()
	var payload linearWorkItemPayload
	if err := json.Unmarshal([]byte(`{"id":"id-`+identifier+`","identifier":"`+identifier+`","title":"T",
		"createdAt":"2026-01-01T00:00:00Z","updatedAt":"2026-11-01T00:00:00Z",
		"state":{"name":"`+state+`","type":"`+stateType+`"},
		"history":{"nodes":`+history+`,"pageInfo":{"hasNextPage":false,"endCursor":""}}}`), &payload); err != nil {
		t.Fatal(err)
	}
	return payload
}

// A close and its reopen at one instant (the same millisecond) are two rows of one sorting key prefix, told apart by
// tie_ordinal. The executors split each destination of an emission into sub-chunks of DefaultChunkPolicy rows, and the
// Linear sink ranks the ties of each sub-chunk it is handed (WriteLinearWorkItemEffect -> rankTransitionTies): when the
// two rows land in two sub-chunks, both take ordinal 0, share one sorting key, and the table keeps one of them. The pair
// sits at rows 99 and 100 of the destination, across the plain 100-row cut: the split must keep it in one sub-chunk.
func TestLinearSameInstantCloseAndReopenKeepTwoKeysAcrossEffectChunks(t *testing.T) {
	now := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	claim := nativeTestClaim("linear", "work-items")
	start := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	var filler []string
	for index := 0; index < 98; index++ {
		from, to := `{"name":"Todo","type":"unstarted"}`, `{"name":"In Progress","type":"started"}`
		if index%2 == 1 {
			from, to = to, from
		}
		filler = append(filler, fmt.Sprintf(`{"id":"f%d","createdAt":"%s","fromState":%s,"toState":%s}`,
			index, start.Add(time.Duration(index)*time.Minute).Format(time.RFC3339Nano), from, to))
	}
	payloads := []linearWorkItemPayload{
		linearCloseReproPayload14570(t, "AAA-1", "Todo", "unstarted", "["+strings.Join(filler, ",")+"]"),
		linearCloseReproPayload14570(t, "ZZZ-5", "Todo", "unstarted", `[
			{"id":"z1","createdAt":"2026-10-01T10:00:00.000Z","fromState":{"name":"Todo","type":"unstarted"},"toState":{"name":"Backlog","type":"backlog"}},
			{"id":"z2","createdAt":"2026-10-15T10:00:00.050Z","fromState":{"name":"Backlog","type":"backlog"},"toState":{"name":"Canceled","type":"canceled"}},
			{"id":"z3","createdAt":"2026-10-15T10:00:00.050Z","fromState":{"name":"Canceled","type":"canceled"},"toState":{"name":"Todo","type":"unstarted"}}]`),
	}
	rows := linearWorkItemRows{}
	for _, payload := range payloads {
		item, transitions, err := normalizeLinearWorkItem(claim, payload, now)
		if err != nil {
			t.Fatal(err)
		}
		rows.WorkItems = append(rows.WorkItems, item)
		rows.StatusTransitions = append(rows.StatusTransitions, transitions...)
	}
	effects, err := buildLinearWorkItemEffectsFromRows(rows)
	if err != nil {
		t.Fatal(err)
	}
	instant := time.Date(2026, 10, 15, 10, 0, 0, 50_000_000, time.UTC)
	if positions := transitionTiePositions14570(t, effects, "linear:ZZZ-5", instant); fmt.Sprint(positions) != "[99 100]" {
		t.Fatalf("setup: the pair sits at rows %v of work_item_transitions, want [99 100] (across the plain 100-row cut)", positions)
	}
	chunks, err := SplitCompleteRouteBatch(CompleteRouteBatch{Effects: effects}, DefaultChunkPolicy())
	if err != nil {
		t.Fatal(err)
	}
	pairChunks := map[int]bool{}
	keys := map[string][]string{}
	for index, chunk := range chunks {
		for _, effect := range chunk.Effects {
			if effect.Destination != "work_item_transitions" || len(effect.Rows) == 0 {
				continue
			}
			decoded, err := decodeEffectRows[linearWorkItemTransitionRow](effect)
			if err != nil {
				t.Fatal(err)
			}
			for _, row := range rankTransitionTies(projectRows(decoded, projectLinearTransition)) {
				if row.WorkItemID == "linear:ZZZ-5" && row.OccurredAt.Equal(instant) {
					pairChunks[index] = true
				}
				key := transitionSortingKey(row)
				keys[key] = append(keys[key], row.FromStatus+"->"+row.ToStatus)
			}
		}
	}
	if len(pairChunks) != 1 {
		t.Errorf("the close and the reopen of linear:ZZZ-5 sit in chunks %v of %d, want one chunk", pairChunks, len(chunks))
	}
	for key, members := range keys {
		if len(members) > 1 {
			t.Errorf("sorting key %q holds %d rows %v: the table keeps one of them", key, len(members), members)
		}
	}
}

// transitionTiePositions14570 returns the positions of an item's rows at one instant in the unsplit work_item_transitions
// effect (canonical row order, the order the split cuts).
func transitionTiePositions14570(t *testing.T, effects []EffectBatch, workItemID string, instant time.Time) []int {
	t.Helper()
	var positions []int
	for _, effect := range effects {
		if effect.Destination != "work_item_transitions" {
			continue
		}
		for index, raw := range effect.Rows {
			var row struct {
				WorkItemID string    `json:"work_item_id"`
				OccurredAt time.Time `json:"occurred_at"`
			}
			if err := json.Unmarshal(raw, &row); err != nil {
				t.Fatal(err)
			}
			if row.WorkItemID == workItemID && clickHouseMillis(row.OccurredAt).Equal(instant) {
				positions = append(positions, index)
			}
		}
	}
	return positions
}

type transitionTieSpec14570 struct {
	item     string
	at       time.Time
	from, to string
}

// transitionTieSpecs14570 is fillers rows of item AAA-1 a minute apart, then item ZZZ-5: a move to Backlog, and a close to
// Canceled and its reopen to Todo in one millisecond (the reopen 0.4 ms later in the source, one stored millisecond).
func transitionTieSpecs14570(fillers int) []transitionTieSpec14570 {
	start := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	var specs []transitionTieSpec14570
	for index := 0; index < fillers; index++ {
		from, to := "Todo", "In Progress"
		if index%2 == 1 {
			from, to = to, from
		}
		specs = append(specs, transitionTieSpec14570{"AAA-1", start.Add(time.Duration(index) * time.Minute), from, to})
	}
	return append(specs,
		transitionTieSpec14570{"ZZZ-5", time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC), "Todo", "Backlog"},
		transitionTieSpec14570{"ZZZ-5", time.Date(2026, 10, 15, 10, 0, 0, 50_000_000, time.UTC), "Backlog", "Canceled"},
		transitionTieSpec14570{"ZZZ-5", time.Date(2026, 10, 15, 10, 0, 0, 50_400_000, time.UTC), "Canceled", "Todo"},
	)
}

func transitionTieStatus14570(raw string) string {
	switch raw {
	case "Todo", "Backlog":
		return "todo"
	case "Canceled":
		return "canceled"
	default:
		return "in_progress"
	}
}

func githubTransitionRows14570(provider, prefix string, specs []transitionTieSpec14570) []githubWorkItemTransitionRow {
	synced := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	rows := make([]githubWorkItemTransitionRow, 0, len(specs))
	for _, spec := range specs {
		from, to, actor := spec.from, spec.to, "dev"
		rows = append(rows, githubWorkItemTransitionRow{
			WorkItemID: prefix + spec.item, Provider: provider, OccurredAt: spec.at,
			FromStatusRaw: &from, ToStatusRaw: &to, FromStatus: transitionTieStatus14570(from),
			ToStatus: transitionTieStatus14570(to), Actor: &actor, OrgID: "org-acme", LastSynced: synced,
		})
	}
	return rows
}

// transitionTieProvider14570 builds a provider's effects with its own route builder and reads one transitions sub-chunk
// the way its sink ranks it (GitLab and Jira write through the GitHub transitions sink).
type transitionTieProvider14570 struct {
	name   string
	prefix string
	build  func(t *testing.T, specs []transitionTieSpec14570) []EffectBatch
	rank   func(t *testing.T, effect EffectBatch) []workItemTransitionStoredRow
}

func rankGitHubTransitionEffect14570(t *testing.T, effect EffectBatch) []workItemTransitionStoredRow {
	t.Helper()
	decoded, err := decodeEffectRows[githubWorkItemTransitionRow](effect)
	if err != nil {
		t.Fatal(err)
	}
	return rankTransitionTies(projectRows(decoded, projectWorkItemTransition))
}

func transitionTieProviders14570() []transitionTieProvider14570 {
	return []transitionTieProvider14570{
		{name: "github", prefix: "gh:acme/app#", rank: rankGitHubTransitionEffect14570,
			build: func(t *testing.T, specs []transitionTieSpec14570) []EffectBatch {
				t.Helper()
				var raw []json.RawMessage
				for _, row := range githubTransitionRows14570("github", "gh:acme/app#", specs) {
					encoded, err := json.Marshal(row)
					if err != nil {
						t.Fatal(err)
					}
					raw = append(raw, encoded)
				}
				effects, err := BuildGitHubWorkItemEffects(GitHubWorkItemEffectRows{WorkItemTransitions: raw})
				if err != nil {
					t.Fatal(err)
				}
				return effects
			}},
		{name: "gitlab", prefix: "gitlab:acme/app#", rank: rankGitHubTransitionEffect14570,
			build: func(t *testing.T, specs []transitionTieSpec14570) []EffectBatch {
				t.Helper()
				effects, err := buildGitLabWorkItemEffectsFromRows(gitlabWorkItemRows{
					StatusTransitions: githubTransitionRows14570("gitlab", "gitlab:acme/app#", specs),
				})
				if err != nil {
					t.Fatal(err)
				}
				return effects
			}},
		{name: "jira", prefix: "jira:", rank: rankGitHubTransitionEffect14570,
			build: func(t *testing.T, specs []transitionTieSpec14570) []EffectBatch {
				t.Helper()
				effects, err := BuildJiraWorkItemEffects(jiraWorkItemRows{
					Transitions: githubTransitionRows14570("jira", "jira:", specs),
				})
				if err != nil {
					t.Fatal(err)
				}
				return effects
			}},
		{name: "linear", prefix: "linear:",
			build: func(t *testing.T, specs []transitionTieSpec14570) []EffectBatch {
				t.Helper()
				var rows linearWorkItemRows
				for _, row := range githubTransitionRows14570("linear", "linear:", specs) {
					rows.StatusTransitions = append(rows.StatusTransitions, linearWorkItemTransitionRow{
						WorkItemID: row.WorkItemID, Provider: row.Provider, OccurredAt: row.OccurredAt,
						FromStatusRaw: row.FromStatusRaw, ToStatusRaw: row.ToStatusRaw, FromStatus: row.FromStatus,
						ToStatus: row.ToStatus, Actor: row.Actor, OrgID: row.OrgID, LastSynced: row.LastSynced,
					})
				}
				effects, err := buildLinearWorkItemEffectsFromRows(rows)
				if err != nil {
					t.Fatal(err)
				}
				return effects
			},
			rank: func(t *testing.T, effect EffectBatch) []workItemTransitionStoredRow {
				t.Helper()
				decoded, err := decodeEffectRows[linearWorkItemTransitionRow](effect)
				if err != nil {
					t.Fatal(err)
				}
				return rankTransitionTies(projectRows(decoded, projectLinearTransition))
			}},
	}
}

// splitTransitionKeys14570 splits the effects and returns, per sorting key the sinks write, the rows that hold it, and the
// sub-chunks that hold rows of the item at the instant.
func splitTransitionKeys14570(
	t *testing.T, provider transitionTieProvider14570, effects []EffectBatch, policy ChunkPolicy, workItemID string, instant time.Time,
) (keys map[string][]string, itemChunks map[int]int, chunkCount int) {
	t.Helper()
	chunks, err := SplitCompleteRouteBatch(CompleteRouteBatch{Effects: effects}, policy)
	if err != nil {
		t.Fatal(err)
	}
	keys, itemChunks = map[string][]string{}, map[int]int{}
	for index, chunk := range chunks {
		for _, effect := range chunk.Effects {
			if effect.Destination != "work_item_transitions" || len(effect.Rows) == 0 {
				continue
			}
			for _, row := range provider.rank(t, effect) {
				if row.WorkItemID == workItemID && row.OccurredAt.Equal(instant) {
					itemChunks[index]++
				}
				key := transitionSortingKey(row)
				keys[key] = append(keys[key], row.FromStatusRaw+"->"+row.ToStatusRaw)
			}
		}
	}
	return keys, itemChunks, len(chunks)
}

var transitionTieInstant14570 = time.Date(2026, 10, 15, 10, 0, 0, 50_000_000, time.UTC)

// Per provider: the close and the reopen of one item in one millisecond sit at rows 99 and 100 of work_item_transitions,
// across the plain 100-row cut. The split keeps them in one sub-chunk, so the sink ranks them 0 and 1: two keys, two rows.
func TestTransitionTiesAtTheSubChunkCutKeepDistinctKeysPerProvider14570(t *testing.T) {
	for _, provider := range transitionTieProviders14570() {
		t.Run(provider.name, func(t *testing.T) {
			item := provider.prefix + "ZZZ-5"
			effects := provider.build(t, transitionTieSpecs14570(98))
			if positions := transitionTiePositions14570(t, effects, item, transitionTieInstant14570); fmt.Sprint(positions) != "[99 100]" {
				t.Fatalf("setup: the pair sits at rows %v of work_item_transitions, want [99 100]", positions)
			}
			keys, itemChunks, count := splitTransitionKeys14570(t, provider, effects, DefaultChunkPolicy(), item, transitionTieInstant14570)
			if len(itemChunks) != 1 || count != 2 {
				t.Errorf("the pair sits in chunks %v of %d, want one chunk of 2", itemChunks, count)
			}
			if len(keys) != 101 {
				t.Errorf("the sinks write %d keys, want 101 (one per row)", len(keys))
			}
			for key, members := range keys {
				if len(members) > 1 {
					t.Errorf("sorting key %q holds %d rows %v: the table keeps one of them", key, len(members), members)
				}
			}
		})
	}
}

// Per provider: the item's rows read in another unit (alone, or after other items' rows that move the cut onto the pair)
// and read again take the same keys with the same rows, so a re-sync replaces each row and adds none.
func TestTransitionTieOrdinalsStableAcrossUnitsAndRereads14570(t *testing.T) {
	for _, provider := range transitionTieProviders14570() {
		t.Run(provider.name, func(t *testing.T) {
			item := provider.prefix + "ZZZ-5"
			itemKeys := func(fillers int) string {
				keys, _, _ := splitTransitionKeys14570(
					t, provider, provider.build(t, transitionTieSpecs14570(fillers)), DefaultChunkPolicy(), item, transitionTieInstant14570,
				)
				var owned []string
				for key, members := range keys {
					if strings.Contains(key, item+workItemKeySeparator) {
						owned = append(owned, fmt.Sprintf("%q=%v", key, members))
					}
				}
				sort.Strings(owned)
				return fmt.Sprint(owned)
			}
			alone, atTheCut, reread := itemKeys(0), itemKeys(98), itemKeys(98)
			if !strings.Contains(alone, `\x1f1"=[Canceled->Todo]`) || !strings.Contains(alone, `\x1f0"=[Backlog->Canceled]`) {
				t.Fatalf("setup: alone the pair takes keys %s, want ordinals 0 and 1", alone)
			}
			if atTheCut != alone || reread != alone {
				t.Errorf("the item's keys move between units: alone %s, at the cut %s, re-read %s", alone, atTheCut, reread)
			}
		})
	}
}

// A split halved to one row per sub-chunk still keeps a tie group whole: the pair takes one sub-chunk of 2 rows, every other
// row one of its own.
func TestTransitionTieGroupLargerThanTheRowCapKeepsOneSubChunk14570(t *testing.T) {
	provider := transitionTieProviders14570()[3]
	item := provider.prefix + "ZZZ-5"
	policy := DefaultChunkPolicy()
	policy.MaxSourceItems, policy.MaxEffectRows = 1, 1
	keys, itemChunks, count := splitTransitionKeys14570(t, provider, provider.build(t, transitionTieSpecs14570(3)), policy, item, transitionTieInstant14570)
	if len(itemChunks) != 1 || count != 5 {
		t.Errorf("the pair sits in chunks %v of %d, want one chunk of 5", itemChunks, count)
	}
	for _, rows := range itemChunks {
		if rows != 2 {
			t.Errorf("the pair's chunk holds %d of its rows, want 2", rows)
		}
	}
	if len(keys) != 6 {
		t.Errorf("the sinks write %d keys, want 6 (one per row)", len(keys))
	}
	pair := provider.build(t, transitionTieSpecs14570(0)[1:])
	if _, itemChunks, count := splitTransitionKeys14570(t, provider, pair, policy, item, transitionTieInstant14570); count != 1 || len(itemChunks) != 1 {
		t.Errorf("the pair alone sits in chunks %v of %d, want one chunk of 1", itemChunks, count)
	}
}

// With no two rows of one item at one millisecond, the transitions split cuts as plain slicing: the sub-chunks hold the
// canonical rows [0, 100) and [100, 150), byte for byte, as before the tie groups.
func TestTransitionSplitWithoutTiesCutsAsPlainSlicing14570(t *testing.T) {
	provider := transitionTieProviders14570()[0]
	specs := transitionTieSpecs14570(150)[:150]
	effects := provider.build(t, specs)
	var canonical []json.RawMessage
	for _, effect := range effects {
		if effect.Destination == "work_item_transitions" {
			canonical = effect.Rows
		}
	}
	chunks, err := SplitCompleteRouteBatch(CompleteRouteBatch{Effects: effects}, DefaultChunkPolicy())
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 2 || len(canonical) != 150 {
		t.Fatalf("setup: %d chunks of %d rows, want 2 of 150", len(chunks), len(canonical))
	}
	for index, chunk := range chunks {
		want := canonical[index*100 : min((index+1)*100, len(canonical))]
		for _, effect := range chunk.Effects {
			if effect.Destination != "work_item_transitions" {
				continue
			}
			if fmt.Sprintf("%s", effect.Rows) != fmt.Sprintf("%s", want) {
				t.Errorf("chunk %d holds rows other than canonical [%d, %d)", index, index*100, index*100+len(want))
			}
		}
	}
}
