package teamscope

import "testing"

// The one rule for the reason a filter that names repositories matched nothing
// (CHAOS-9098): every case of named x resolved x team scope x held by the team.
func TestClassifyEmptyFilterIsTheOneRule(t *testing.T) {
	notInTeam, notFound := ReasonRepositoryNotInTeam, ReasonRepositoryNotFound
	cases := []struct {
		name                string
		named, resolved     int
		teamScope           bool
		ownedByTeam         int
		want                *EmptyReason
		explanationForTests string
	}{
		{"no repository named", 0, 0, false, 0, nil, "nothing to explain"},
		{"no repository named, a team scope", 0, 0, true, 0, nil, "a team scope alone is not a repository filter"},
		{"named, resolved, no team scope", 2, 2, false, 0, nil, "something matched"},
		{"named, none resolved, no team scope", 2, 0, false, 0, &notFound, "a named repository resolved to nothing"},
		{"one named resolved, one not, no team scope", 2, 1, false, 0, nil, "the value covers the part that matched"},
		{"all named resolved, all held", 2, 2, true, 2, nil, "something matched"},
		{"all named resolved, part held", 2, 2, true, 1, nil, "only part is in the team: null"},
		{"all named resolved, none held", 2, 2, true, 0, &notInTeam, "every named repository exists, the team holds none"},
		{"one resolved and held, one unresolved", 2, 1, true, 1, nil, "the part that matched"},
		{"one resolved and not held, one unresolved", 2, 1, true, 0, &notFound, "nothing matched and a named repository resolved to nothing"},
		{"none resolved, a team scope", 2, 0, true, 0, &notFound, "a named repository resolved to nothing"},
		{"duplicate references resolved to one repository, held", 2, 2, true, 1, nil, "something matched"},
	}
	for _, c := range cases {
		got := ClassifyEmptyFilter(c.named, c.resolved, c.teamScope, c.ownedByTeam)
		if (got == nil) != (c.want == nil) || (got != nil && *got != *c.want) {
			t.Errorf("%s: named %d resolved %d team %t held %d = %v, want %v (%s)", c.name, c.named, c.resolved, c.teamScope, c.ownedByTeam, deref(got), deref(c.want), c.explanationForTests)
		}
	}
}

func deref(reason *EmptyReason) string {
	if reason == nil {
		return "null"
	}
	return string(*reason)
}

// The wire words are the ticket's, and nil stays nil.
func TestEmptyReasonWireWords(t *testing.T) {
	if string(ReasonRepositoryNotInTeam) != "repository_not_in_team" || string(ReasonRepositoryNotFound) != "repository_not_found" {
		t.Errorf("wire words changed: %q %q", ReasonRepositoryNotInTeam, ReasonRepositoryNotFound)
	}
	if EmptyReasonText(nil) != nil {
		t.Error("a nil reason must stay nil on the wire")
	}
	reason := ReasonRepositoryNotFound
	if text := EmptyReasonText(&reason); text == nil || *text != "repository_not_found" {
		t.Errorf("EmptyReasonText = %v", text)
	}
}
