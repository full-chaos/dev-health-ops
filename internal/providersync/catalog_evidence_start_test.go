package providersync

import (
	"testing"
	"time"
)

func catalogTestTime(day int) time.Time {
	return time.Date(2026, 9, day, 12, 0, 0, 0, time.UTC)
}

func catalogTestRow(team, key string, from time.Time, to *time.Time, updated time.Time) catalogEvidenceRow {
	spec := catalogEvidenceSpecs[CatalogEvidenceProjectOwnership]
	values := make([]any, 12)
	values[spec.validFrom], values[spec.validTo], values[spec.updatedAt] = from, to, updated
	return catalogEvidenceRow{Fact: CatalogEvidenceFact{TeamID: team, Key: key}, ValidFrom: from, ValidTo: to, UpdatedAt: updated, values: values}
}

func catalogTestEvidence(byKey map[string]time.Time) catalogEvidence {
	return catalogEvidence{byKey: byKey}
}

// TestCatalogEvidenceCarryMovesTheFirstRowOfAFact is the decision table of the
// carry: which row of a fact moves to the evidence, and when nothing moves.
func TestCatalogEvidenceCarryMovesTheFirstRowOfAFact(t *testing.T) {
	at := catalogTestTime(20)
	sync, later, evidence := catalogTestTime(10), catalogTestTime(15), catalogTestTime(1)
	closed := catalogTestTime(12)
	emptyAt := sync
	cells := []struct {
		name     string
		rows     []catalogEvidenceRow
		evidence map[string]time.Time
		want     []catalogEvidenceMove
		newer    int
	}{
		{"open row at the sync moves to the evidence",
			[]catalogEvidenceRow{catalogTestRow("t", "p", sync, nil, sync)},
			map[string]time.Time{"p": evidence}, []catalogEvidenceMove{{Row: 0, From: evidence}}, 0},
		{"a closed first row moves too, its end kept",
			[]catalogEvidenceRow{catalogTestRow("t", "p", sync, &closed, closed)},
			map[string]time.Time{"p": evidence}, []catalogEvidenceMove{{Row: 0, From: evidence}}, 0},
		{"no evidence keeps the start",
			[]catalogEvidenceRow{catalogTestRow("t", "p", sync, nil, sync)}, nil, nil, 0},
		{"evidence after the start keeps the start",
			[]catalogEvidenceRow{catalogTestRow("t", "p", sync, nil, sync)},
			map[string]time.Time{"p": later}, nil, 0},
		{"evidence after the run is no evidence",
			[]catalogEvidenceRow{catalogTestRow("t", "p", sync, nil, sync)},
			map[string]time.Time{"p": at.Add(time.Hour)}, nil, 0},
		{"evidence at the epoch is no evidence",
			[]catalogEvidenceRow{catalogTestRow("t", "p", sync, nil, sync)},
			map[string]time.Time{"p": time.Unix(0, 0).UTC()}, nil, 0},
		{"a row already at the evidence writes nothing",
			[]catalogEvidenceRow{catalogTestRow("t", "p", evidence, nil, at), catalogTestRow("t", "p", sync, &emptyAt, at)},
			map[string]time.Time{"p": evidence}, nil, 0},
		{"a returning member: only the first interval moves",
			[]catalogEvidenceRow{catalogTestRow("t", "p", later, nil, later), catalogTestRow("t", "p", sync, &closed, closed)},
			map[string]time.Time{"p": evidence}, []catalogEvidenceMove{{Row: 1, From: evidence}}, 0},
		{"an empty interval is no first row",
			[]catalogEvidenceRow{catalogTestRow("t", "p", catalogTestTime(2), &[]time.Time{catalogTestTime(2)}[0], sync), catalogTestRow("t", "p", sync, nil, sync)},
			map[string]time.Time{"p": evidence}, []catalogEvidenceMove{{Row: 1, From: evidence}}, 0},
		{"a fact with a row newer than the run is another run's",
			[]catalogEvidenceRow{catalogTestRow("t", "p", sync, nil, at.Add(time.Minute))},
			map[string]time.Time{"p": evidence}, nil, 1},
		{"each fact of a key moves on its own",
			[]catalogEvidenceRow{catalogTestRow("a", "p", sync, nil, sync), catalogTestRow("b", "p", later, nil, later)},
			map[string]time.Time{"p": evidence}, []catalogEvidenceMove{{Row: 0, From: evidence}, {Row: 1, From: evidence}}, 0},
	}
	for _, cell := range cells {
		t.Run(cell.name, func(t *testing.T) {
			plan := planCatalogEvidenceCarry(cell.rows, catalogTestEvidence(cell.evidence), at)
			if len(plan.moves) != len(cell.want) {
				t.Fatalf("moves %+v, want %+v", plan.moves, cell.want)
			}
			for index, move := range plan.moves {
				if move.Row != cell.want[index].Row || !move.From.Equal(cell.want[index].From) {
					t.Fatalf("move %d = %+v, want %+v", index, move, cell.want[index])
				}
			}
			if plan.newer != cell.newer {
				t.Fatalf("newer %d, want %d", plan.newer, cell.newer)
			}
		})
	}
}

// TestCatalogEvidenceCarryRetiresTheOldKeyAsAnEmptyInterval: the moved row keeps
// every column but valid_from and updated_at; its old key is written again with
// valid_to = valid_from and a version past the stored one.
func TestCatalogEvidenceCarryRetiresTheOldKeyAsAnEmptyInterval(t *testing.T) {
	spec := catalogEvidenceSpecs[CatalogEvidenceProjectOwnership]
	at, sync, evidence, closed := catalogTestTime(20), catalogTestTime(10), catalogTestTime(1), catalogTestTime(12)
	for _, cell := range []struct {
		name        string
		row         catalogEvidenceRow
		wantVersion time.Time
	}{
		{"open row", catalogTestRow("t", "p", sync, nil, sync), at},
		{"closed row", catalogTestRow("t", "p", sync, &closed, closed), at},
		{"row stored at the run", catalogTestRow("t", "p", sync, nil, at), at.Add(time.Millisecond)},
	} {
		t.Run(cell.name, func(t *testing.T) {
			cell.row.values[0] = "org"
			out := catalogEvidenceCarryRows(spec, []catalogEvidenceRow{cell.row}, []catalogEvidenceMove{{Row: 0, From: evidence}}, at)
			if len(out) != 2 {
				t.Fatalf("%d rows, want the moved row and the retired key", len(out))
			}
			moved, retired := out[0], out[1]
			if moved[0] != "org" || retired[0] != "org" {
				t.Fatalf("a column other than the interval changed: %v / %v", moved[0], retired[0])
			}
			if got := moved[spec.validFrom].(time.Time); !got.Equal(evidence) {
				t.Fatalf("moved valid_from %s, want the evidence %s", got, evidence)
			}
			if got := moved[spec.validTo].(*time.Time); (got == nil) != (cell.row.ValidTo == nil) || (got != nil && !got.Equal(*cell.row.ValidTo)) {
				t.Fatalf("moved valid_to %v, want the row's own %v", got, cell.row.ValidTo)
			}
			if got := moved[spec.updatedAt].(time.Time); !got.Equal(at) {
				t.Fatalf("moved updated_at %s, want the run %s", got, at)
			}
			if got := retired[spec.validFrom].(time.Time); !got.Equal(sync) {
				t.Fatalf("retired valid_from %s, want the old key %s", got, sync)
			}
			if got := retired[spec.validTo].(*time.Time); got == nil || !got.Equal(sync) {
				t.Fatalf("retired valid_to %v, want an empty interval at %s", got, sync)
			}
			if got := retired[spec.updatedAt].(time.Time); !got.Equal(cell.wantVersion) {
				t.Fatalf("retired updated_at %s, want %s", got, cell.wantVersion)
			}
		})
	}
}

// TestCatalogEvidenceStartOfANewFact: a fact the scope holds no row of starts at
// min(evidence, the writer's start); a fact it holds keeps the writer's start.
func TestCatalogEvidenceStartOfANewFact(t *testing.T) {
	at, sync, evidence := catalogTestTime(20), catalogTestTime(10), catalogTestTime(1)
	emptyAt := catalogTestTime(5)
	rows := []catalogEvidenceRow{
		catalogTestRow("t", "held", sync, nil, sync),
		catalogTestRow("t", "voided", emptyAt, &emptyAt, emptyAt),
	}
	evidenceOf := catalogTestEvidence(map[string]time.Time{"held": evidence, "new": evidence, "voided": evidence, "late": catalogTestTime(25)})
	plan := planCatalogEvidenceCarry(rows, evidenceOf, at)
	starts := CatalogEvidenceStarts{at: at, known: plan.known, evidence: evidenceOf, usable: true}
	for _, cell := range []struct {
		key  string
		want time.Time
	}{
		{"new", evidence},
		{"voided", evidence},
		{"held", at},
		{"late", at},
		{"none", at},
	} {
		if got := starts.Start(CatalogEvidenceFact{TeamID: "t", Key: cell.key}, at); !got.Equal(cell.want) {
			t.Errorf("%s: start %s, want %s", cell.key, got, cell.want)
		}
	}
	if got := (CatalogEvidenceStarts{}).Start(CatalogEvidenceFact{TeamID: "t", Key: "new"}, at); !got.Equal(at) {
		t.Errorf("a carry that read nothing moved a start: %s", got)
	}
}

// TestCatalogPeopleFollowTheMatrixMemberRule: a member's earliest work is a
// work item whose reporter or assignee is the member under the attribution
// matrix's rule (the member id after its provider prefix: equal, after a ':',
// or before an '@') or one of its identities.
func TestCatalogPeopleFollowTheMatrixMemberRule(t *testing.T) {
	people := newCatalogPeople(map[string]time.Time{
		"github:Alice-FX":               catalogTestTime(3),
		"alice-fx@corp.example":         catalogTestTime(2),
		"Bob-FX":                        catalogTestTime(4),
		"xalice-fx":                     catalogTestTime(1),
		"carol@fixtures.example":        catalogTestTime(5),
		"jira:accountid:712020:fx-hana": catalogTestTime(6),
		"dave":                          time.Unix(0, 0).UTC(),
	})
	for _, cell := range []struct {
		name string
		fact CatalogEvidenceFact
		want time.Time
	}{
		{"suffix and @ prefix, earliest wins", MembershipEvidenceFact("gh:t", "gh:alice-fx", nil, nil, nil), catalogTestTime(2)},
		{"equal, any case", MembershipEvidenceFact("gh:t", "gh:bob-fx", nil, nil, nil), catalogTestTime(4)},
		{"an identity facet", MembershipEvidenceFact("gh:t", "gh:carol-fx", nil, nil, []string{"github:carol-fx", "carol@fixtures.example"}), catalogTestTime(5)},
		{"a member id with a ':' in its facet", MembershipEvidenceFact("jira:t", "jira:712020:fx-hana", nil, nil, nil), catalogTestTime(6)},
		{"epoch is no work", MembershipEvidenceFact("gh:t", "gh:dave", nil, nil, nil), time.Time{}},
		{"no work", MembershipEvidenceFact("gh:t", "gh:erin-fx", nil, nil, nil), time.Time{}},
	} {
		if got := people.of(cell.fact); !got.Equal(cell.want) {
			t.Errorf("%s: %s, want %s", cell.name, got, cell.want)
		}
	}
}

// TestCatalogRepoEvidenceByIDOrName: a repository row with an id takes the
// earliest pull request of that id; a row without one is matched by name.
func TestCatalogRepoEvidenceByIDOrName(t *testing.T) {
	at := catalogTestTime(20)
	id := "1A7DD61A-91DB-B720-92FF-1AB5A194DFE4"
	evidence := catalogEvidence{byRepo: map[string]time.Time{
		"1a7dd61a-91db-b720-92ff-1ab5a194dfe4": catalogTestTime(3),
		catalogRepoNameKey("Org/Payments"):     catalogTestTime(4),
	}}
	if got, ok := evidence.of(RepoEvidenceFact("gh:t", "org/other", &id), at); !ok || !got.Equal(catalogTestTime(3)) {
		t.Errorf("by id: %s %v", got, ok)
	}
	if got, ok := evidence.of(RepoEvidenceFact("gh:t", "org/payments", nil), at); !ok || !got.Equal(catalogTestTime(4)) {
		t.Errorf("by name: %s %v", got, ok)
	}
	if _, ok := evidence.of(RepoEvidenceFact("gh:t", "org/none", nil), at); ok {
		t.Error("a repository with no pull request has evidence")
	}
}
