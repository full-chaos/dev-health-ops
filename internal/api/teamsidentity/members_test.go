package teamsidentity

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/api/pytime"
)

func strPtr(s string) *string { return &s }

func identityFixture(canonical string, display, email *string, providers map[string][]string) Identity {
	dict := pybody.NewOrderedStringListDict()
	for key, values := range providers {
		dict.Set(key, values)
	}
	id := uuid.NewSHA1(uuid.NameSpaceOID, []byte(canonical))
	return Identity{ID: id.String(), IdentityUUID: id, CanonicalID: canonical, DisplayName: display, Email: email, ProviderIdentities: dict,
		IsActive: true, UpdatedAt: time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)}
}

func TestMatchMembersTiers(t *testing.T) {
	candidates := []Identity{
		identityFixture("by-provider", strPtr("Zed"), nil, map[string][]string{"github": {"gh-1"}}),
		identityFixture("by-email", nil, strPtr("e@example.com"), nil),
		identityFixture("by-name", strPtr("Member 2x"), nil, nil),
	}
	members := []discoveredMember{
		{ProviderType: "github", ProviderIdentity: "gh-1", Email: strPtr("e@example.com")},                    // provider beats email
		{ProviderType: "github", ProviderIdentity: "other", Email: strPtr("e@example.com")},                   // email
		{ProviderType: "github", ProviderIdentity: "n", DisplayName: strPtr("MEMBER 2")},                      // name, case-insensitive
		{ProviderType: "github", ProviderIdentity: "far", DisplayName: strPtr("Completely Different")},        // below the threshold
		{ProviderType: "gitlab", ProviderIdentity: "gh-1"},                                                    // provider identity under another provider
		{ProviderType: "github", ProviderIdentity: "empty-email", Email: strPtr(""), DisplayName: strPtr("")}, // empty values never match
	}
	got, err := matchMembers(members, candidates)
	if err != nil {
		t.Fatal(err)
	}
	type want struct {
		status, canonical, reason string
		confidence                float64
	}
	expected := []want{
		{"matched", "by-provider", "", 1.0},
		{"suggested", "by-email", "email_match", 0.95},
		{"suggested", "by-name", "display_name_similarity", 0.94},
		{"unmatched", "", "", 0},
		{"unmatched", "", "", 0},
		{"unmatched", "", "", 0},
	}
	for index, w := range expected {
		m := got[index]
		if m.Status != w.status {
			t.Errorf("member %d: status %q, want %q", index, m.Status, w.status)
			continue
		}
		if w.canonical == "" {
			if m.Matched != nil || m.Confidence != nil || m.SuggestionReason != nil {
				t.Errorf("member %d: unmatched must carry nothing, got %+v", index, m)
			}
			continue
		}
		if m.Matched == nil || m.Matched.CanonicalID != w.canonical {
			t.Errorf("member %d: matched %+v, want %s", index, m.Matched, w.canonical)
		}
		if m.Confidence == nil || *m.Confidence != w.confidence {
			t.Errorf("member %d: confidence %v, want %v", index, m.Confidence, w.confidence)
		}
		if w.reason != "" && (m.SuggestionReason == nil || *m.SuggestionReason != w.reason) {
			t.Errorf("member %d: reason %v, want %s", index, m.SuggestionReason, w.reason)
		}
	}
}

func TestMatchMembersNameThresholdIsInclusiveAt80(t *testing.T) {
	// "abcde" vs "abcdx": 2*4/10 = 0.8 exactly -> suggested; "abxxx": 0.6 -> not.
	candidates := []Identity{identityFixture("c", strPtr("abcdx"), nil, nil)}
	at, _ := matchMembers([]discoveredMember{{ProviderType: "github", ProviderIdentity: "a", DisplayName: strPtr("abcde")}}, candidates)
	if at[0].Status != "suggested" || *at[0].Confidence != 0.8 {
		t.Errorf("ratio 0.8 must suggest at confidence 0.8, got %+v", at[0])
	}
	below, _ := matchMembers([]discoveredMember{{ProviderType: "github", ProviderIdentity: "a", DisplayName: strPtr("abxxx")}}, candidates)
	if below[0].Status != "unmatched" {
		t.Errorf("ratio 0.6 must not match, got %+v", below[0])
	}
}

func TestMatchMembersKeepsTheFirstOfEqualScores(t *testing.T) {
	candidates := []Identity{
		identityFixture("first", strPtr("Same Name"), nil, nil),
		identityFixture("second", strPtr("Same Name"), nil, nil),
	}
	got, _ := matchMembers([]discoveredMember{{ProviderType: "github", ProviderIdentity: "a", DisplayName: strPtr("Same Name")}}, candidates)
	if got[0].Matched.CanonicalID != "first" {
		t.Errorf("a tie keeps the earlier candidate, got %s", got[0].Matched.CanonicalID)
	}
}

func TestPyQuoteNoSafe(t *testing.T) {
	cases := map[string]string{"plain-Slug_1.x~": "plain-Slug_1.x~", "a b": "a%20b", "gh:team/x": "gh%3Ateam%2Fx", "é": "%C3%A9", "": ""}
	for in, want := range cases {
		if got := pyQuoteNoSafe(in); got != want {
			t.Errorf("pyQuoteNoSafe(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestConfidenceForCount(t *testing.T) {
	for count, want := range map[int]string{0: "peripheral", 1: "peripheral", 2: "active", 4: "active", 5: "core", 50: "core"} {
		if got := confidenceForCount(count); got != want {
			t.Errorf("confidenceForCount(%d) = %q, want %q", count, got, want)
		}
	}
}

func TestParseJiraDatetimeAndStampJSON(t *testing.T) {
	cases := map[string]string{
		"2026-09-01T10:00:00.000+0000":        "2026-09-01T10:00:00Z",
		"2026-09-01T10:00:00Z":                "2026-09-01T10:00:00Z",
		"2026-09-01T10:00:00.5+05:30":         "2026-09-01T10:00:00.500000+05:30",
		"2026-09-01T10:00:00-0130":            "2026-09-01T10:00:00-01:30",
		"2026-09-01T10:00:00":                 "2026-09-01T10:00:00",
		"2026-09-01T10:00:00+05:30:15.5":      "2026-09-01T10:00:00+05:30",
		"2026-09-01T10:00:00-00:00:00.000001": "2026-09-01T10:00:00Z",
	}
	for in, want := range cases {
		stamp := parseJiraDatetime(in)
		if stamp == nil {
			t.Errorf("%q did not parse", in)
			continue
		}
		if got := pytime.Pydantic(*stamp); got != want {
			t.Errorf("%q rendered %v, want %s", in, got, want)
		}
	}
	for _, bad := range []string{"", "nope", "2026-02-30T10:00:00", "2026-09-01T25:00:00"} {
		if parseJiraDatetime(bad) != nil {
			t.Errorf("%q must not parse", bad)
		}
	}
	if parseJiraDatetime(nil) != nil || parseJiraDatetime(pyjson.Int{}) != nil {
		t.Error("a non-string is None")
	}
}

func TestConfirmBodiesReportPydanticShapes(t *testing.T) {
	decode := func(text string) *pyjson.Object {
		value, err := pyjson.DecodeString(text)
		if err != nil {
			t.Fatal(err)
		}
		return value.(*pyjson.Object)
	}
	_, links, problems := parseConfirmMembersBody(decode(`{"team_id":"t","links":[{"provider_identity":"x","provider":"linear","canonical_id":"c","action":"merge"}]}`))
	if len(links) != 0 || len(problems) != 2 {
		t.Fatalf("want two pattern errors and no links, got %d links, %d errors", len(links), len(problems))
	}
	if problems[0].Type != "string_pattern_mismatch" || problems[0].Loc[2] != int64(0) || problems[0].Loc[3] != "provider" {
		t.Errorf("first error %+v", problems[0])
	}
	_, members, problems := parseConfirmInferredBody(decode(`{"team_id":"t","members":[{"account_id":"a","action":"skip"},{"account_id":"b","action":"nope"},{"action":"add"}]}`))
	if len(members) != 1 || members[0].AccountID != "a" {
		t.Errorf("only the valid member survives, got %+v", members)
	}
	if len(problems) != 2 || problems[0].Type != "literal_error" || problems[1].Type != "missing" {
		t.Errorf("errors %+v", problems)
	}
	if _, members, problems = parseConfirmInferredBody(decode(`{"team_id":"t"}`)); len(members) != 0 || len(problems) != 0 {
		t.Errorf("members defaults to empty: %+v %+v", members, problems)
	}
}

func TestLinkedProvidersUnionsAndKeepsOrder(t *testing.T) {
	existing := identityFixture("c", nil, nil, nil)
	existing.ProviderIdentities.Set("zeta", []string{"z"})
	existing.ProviderIdentities.Set("github", []string{"b", "a"})
	got := linkedProviders(&existing, "github", "c")
	if keys := got.Keys; len(keys) != 2 || keys[0] != "zeta" || keys[1] != "github" {
		t.Fatalf("existing key order must stay: %v", keys)
	}
	if values, _ := got.Get("github"); len(values) != 3 || values[0] != "a" || values[1] != "b" || values[2] != "c" {
		t.Errorf("github values %v", values)
	}
	if values, _ := existing.ProviderIdentities.Get("github"); len(values) != 2 {
		t.Errorf("the stored dict must not be mutated: %v", values)
	}
	fresh := linkedProviders(nil, "jira", "")
	if values, _ := fresh.Get("jira"); len(values) != 1 || values[0] != "" {
		t.Errorf("an empty identity is kept, as a Python set keeps it: %v", values)
	}
}

func TestBatchClaimDetectsADivergentCanonical(t *testing.T) {
	ownership := &batchOwnership{seen: map[[2]string]string{}}
	if detail := ownership.claim("one", "jira", "acc"); detail != "" {
		t.Fatalf("first claim is free: %s", detail)
	}
	if detail := ownership.claim("one", "jira", "acc"); detail != "" {
		t.Fatalf("the same canonical again is idempotent: %s", detail)
	}
	want := "Provider identity 'jira:acc' is claimed by two different canonical identities in the same request ('one' and 'two')"
	if detail := ownership.claim("two", "jira", "acc"); detail != want {
		t.Errorf("detail %q, want %q", detail, want)
	}
	if detail := ownership.claim("two", "gitlab", "acc"); detail != "" {
		t.Errorf("the same identity under another provider is a different key: %s", detail)
	}
}

func TestJiraTimestampsOrderByTheMomentNotTheClock(t *testing.T) {
	later := parseJiraDatetime("2026-09-06T08:00:00+0000")
	earlier := parseJiraDatetime("2026-09-06T10:00:00+14:00")
	if later == nil || earlier == nil {
		t.Fatal("both timestamps parse")
	}
	wall := func(d *pytime.DateTime) time.Time {
		return d.Time.Add(time.Duration(d.Offset) * time.Second)
	}
	if !wall(earlier).After(wall(later)) {
		t.Fatal("the fixture needs the later wall clock on the earlier instant")
	}
	if !later.Time.After(earlier.Time) {
		t.Errorf("08:00Z must be after 10:00+14:00: %v vs %v", later.Time, earlier.Time)
	}
}

func TestJiraStrptimeFallback(t *testing.T) {
	// fromisoformat refuses these; strptime's one-digit fields, leading-space
	// day and lower-case T accept them.
	for text, want := range map[string]string{
		"2026-9-1T1:2:3+0000":        "2026-09-01T01:02:03Z",
		"2026-09- 1T10:00:00+0100":   "2026-09-01T10:00:00+01:00",
		"2026-09-01t10:00:00.5+0000": "2026-09-01T10:00:00.500000Z",
	} {
		parsed := parseJiraDatetime(text)
		if parsed == nil {
			t.Errorf("%q must parse through strptime", text)
			continue
		}
		if got := pytime.Pydantic(*parsed); got != want {
			t.Errorf("%q rendered %s, want %s", text, got, want)
		}
	}
	for _, bad := range []string{"2026-9-1T1:2:3", "2026-02-30T1:2:3+0000", "2026-9-1T1:2:61+0000", "2026-9-1T1:2:3+2400"} {
		if parseJiraDatetime(bad) != nil {
			t.Errorf("%q must not parse", bad)
		}
	}
}
