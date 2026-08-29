package providersync

import (
	"testing"
	"time"
)

// team_drift_json_test.go pins pyCanonicalJSON/pyStrDatetime/changeIDForTeamField
// against values computed by ACTUALLY RUNNING Python 3's json.dumps(...,
// sort_keys=True, separators=(",", ":"), default=str) and hashlib.sha256 --
// not hand-derived -- so a divergence in escaping, key ordering, or the
// datetime microseconds-omitted-when-zero quirk fails loudly here instead of
// only showing up as an unexplained pending-row duplication after cutover.
// The exact python3 invocation used to produce every expected string below
// is recorded in each test's doc comment for reproduction.
func TestPyCanonicalJSONMatchesPythonJSONDumpsAsciiString(t *testing.T) {
	// python3: json.dumps("hello world", sort_keys=True, separators=(",", ":"))
	got := pyCanonicalJSON("hello world")
	want := `"hello world"`
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestPyCanonicalJSONMatchesPythonJSONDumpsNonAsciiString(t *testing.T) {
	// python3: json.dumps("café 日本\U0001F600", sort_keys=True, separators=(",", ":"))
	// ensure_ascii=True (the default) escapes every non-ASCII codepoint,
	// astral ones (the emoji) as a UTF-16 surrogate pair -- this is the
	// property Go's encoding/json does NOT replicate on its own.
	got := pyCanonicalJSON("café 日本\U0001F600")
	// python3: json.dumps("café 日本\U0001F600", sort_keys=True, separators=(",", ":"))
	// => "café 日本😀" -- a raw Go string literal
	// (backtick) so these six backslash-u sequences are the LITERAL ASCII
	// text ensure_ascii=True emits, not Go source syntax for the original
	// runes.
	want := `"caf\u00e9 \u65e5\u672c\ud83d\ude00"`
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestPyCanonicalJSONMatchesPythonJSONDumpsEmptyAndDedupedSortedList(t *testing.T) {
	// python3: json.dumps([], ...) and json.dumps(sorted(set(["b","a","b","c"])), ...)
	if got, want := pyCanonicalJSON(pyComparisonListField(nil)), `[]`; got != want {
		t.Fatalf("empty: got %q want %q", got, want)
	}
	if got, want := pyCanonicalJSON(pyComparisonListField([]string{"b", "a", "b", "c"})), `["a","b","c"]`; got != want {
		t.Fatalf("deduped+sorted: got %q want %q", got, want)
	}
}

func TestPyCanonicalJSONMatchesPythonJSONDumpsNone(t *testing.T) {
	got := pyCanonicalJSON(nil)
	want := `null`
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestPyStrDatetimeMatchesPythonStrOnAwareUTCDatetime(t *testing.T) {
	// python3: str(datetime(2026, 8, 29, 8, 39, 1, 77000, tzinfo=timezone.utc))
	withMicros := time.Date(2026, 8, 29, 8, 39, 1, 77000*1000, time.UTC)
	if got, want := pyStrDatetime(withMicros), "2026-08-29 08:39:01.077000+00:00"; got != want {
		t.Fatalf("with micros: got %q want %q", got, want)
	}
	// python3: str(datetime(2026, 8, 29, 8, 39, 1, 0, tzinfo=timezone.utc))
	// CPython's str(datetime) OMITS the microseconds component entirely
	// when it is exactly zero -- this is the quirk a naive
	// always-print-microseconds port would miss.
	noMicros := time.Date(2026, 8, 29, 8, 39, 1, 0, time.UTC)
	if got, want := pyStrDatetime(noMicros), "2026-08-29 08:39:01+00:00"; got != want {
		t.Fatalf("no micros: got %q want %q", got, want)
	}
}

func TestChangeIDForTeamFieldMatchesPythonChangeIDForTeamField(t *testing.T) {
	// python3:
	//   payload = {"org_id": "org-1", "entity_type": "team", "entity_id": "team-1",
	//              "change_type": "field_changed", "field": "name",
	//              "old_value_json": json.dumps("Old Name", sort_keys=True, separators=(",", ":")),
	//              "new_value_json": json.dumps("New Name", sort_keys=True, separators=(",", ":"))}
	//   encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
	//   hashlib.sha256(encoded.encode("utf-8")).hexdigest()
	oldJSON := pyCanonicalJSON("Old Name")
	newJSON := pyCanonicalJSON("New Name")
	if oldJSON != `"Old Name"` || newJSON != `"New Name"` {
		t.Fatalf("precondition failed: oldJSON=%q newJSON=%q", oldJSON, newJSON)
	}
	got := changeIDForTeamField("org-1", "team-1", "name", oldJSON, newJSON)
	want := "444c949252f95bfe0941c79794ea4c9fae8c40ab5aeba74b2d5b9e03d853acda"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestPyCanonicalJSONMatchesPythonJSONDumpsNestedDictWithDatetimeAndNull(t *testing.T) {
	// python3 (see conflictDetailForMembership's manual_membership shape):
	//   dt = datetime(2026, 8, 29, 8, 39, 1, 77000, tzinfo=timezone.utc)
	//   nested = {"field": "team_memberships", "manual_membership": {
	//       "org_id": "org-1", "provider": "github", "team_id": "gh:team-a", "member_id": "gh:alice",
	//       "raw_provider_user_id": None, "raw_email": "alice@example.com", "source": "manual",
	//       "is_primary": 1, "specificity": 0, "priority": 0,
	//       "valid_from": dt, "valid_to": None, "updated_at": dt}}
	//   json.dumps(nested, sort_keys=True, separators=(",", ":"), default=str)
	dt := time.Date(2026, 8, 29, 8, 39, 1, 77000*1000, time.UTC)
	nested := map[string]any{
		"field": "team_memberships",
		"manual_membership": map[string]any{
			"org_id": "org-1", "provider": "github", "team_id": "gh:team-a", "member_id": "gh:alice",
			"raw_provider_user_id": (*string)(nil), "raw_email": "alice@example.com", "source": "manual",
			"is_primary": uint8(1), "specificity": uint16(0), "priority": int32(0),
			"valid_from": dt, "valid_to": (*time.Time)(nil), "updated_at": dt,
		},
	}
	got := pyCanonicalJSON(nested)
	want := `{"field":"team_memberships","manual_membership":{"is_primary":1,"member_id":"gh:alice","org_id":"org-1","priority":0,"provider":"github","raw_email":"alice@example.com","raw_provider_user_id":null,"source":"manual","specificity":0,"team_id":"gh:team-a","updated_at":"2026-08-29 08:39:01.077000+00:00","valid_from":"2026-08-29 08:39:01.077000+00:00","valid_to":null}}`
	if got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
}
