package artifactexclude

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

func listOf(items ...pyjson.Value) []pyjson.Value { return items }

func TestArtifactExcludeParseBounds(t *testing.T) {
	// The bounds are literals here, not the constants, so a changed constant fails this cell.
	sixteen := make([]pyjson.Value, 16)
	seventeen := make([]pyjson.Value, 17)
	for index := range seventeen {
		seventeen[index] = "p" + strings.Repeat("x", index)
		if index < 16 {
			sixteen[index] = seventeen[index]
		}
	}
	longest := strings.Repeat("a", 100)
	cells := []struct {
		name      string
		value     pyjson.Value
		want      []string
		clear     bool
		refuseAt  int
		refusal   bool
		forbidden string
	}{
		{name: "three real-shaped prefixes", value: listOf("ci-images-", "ci-timings-", "ci-part-"), want: []string{"ci-images-", "ci-timings-", "ci-part-"}},
		{name: "null clears", value: nil, clear: true},
		{name: "empty list clears", value: listOf(), clear: true},
		{name: "max entries", value: sixteen, want: func() []string {
			out := make([]string, 16)
			for index, item := range sixteen {
				out[index] = item.(string)
			}
			return out
		}()},
		{name: "one entry over max", value: seventeen, refusal: true, refuseAt: -1},
		{name: "max entry bytes", value: listOf(longest), want: []string{longest}},
		{name: "one byte over max", value: listOf("ok", longest+"b"), refusal: true, refuseAt: 1, forbidden: longest},
		{name: "empty entry", value: listOf("ok", ""), refusal: true, refuseAt: 1},
		{name: "space", value: listOf("ci images"), refusal: true, refuseAt: 0, forbidden: "ci images"},
		{name: "slash", value: listOf("ci/images"), refusal: true, refuseAt: 0, forbidden: "ci/images"},
		{name: "glob", value: listOf("ci-*"), refusal: true, refuseAt: 0, forbidden: "ci-*"},
		{name: "non-ascii look-alike", value: listOf("ci-іmages"), refusal: true, refuseAt: 0, forbidden: "іmages"},
		{name: "control character", value: listOf("ci\n"), refusal: true, refuseAt: 0},
		{name: "case-insensitive duplicate", value: listOf("ci-images-", "CI-IMAGES-"), refusal: true, refuseAt: 1, forbidden: "CI-IMAGES-"},
		{name: "not a string", value: listOf("ok", pyjson.IntOf(3)), refusal: true, refuseAt: 1},
		{name: "not a list", value: "ci-images-", refusal: true, refuseAt: -1, forbidden: "ci-images-"},
		{name: "an object", value: pyjson.NewObject(), refusal: true, refuseAt: -1},
	}
	for _, cell := range cells {
		change, err := Parse("github", cell.value)
		if cell.refusal {
			var refused *Refusal
			if !errors.As(err, &refused) {
				t.Errorf("%s: err=%v, want a refusal", cell.name, err)
				continue
			}
			if refused.Index != cell.refuseAt {
				t.Errorf("%s: refusal index %d, want %d", cell.name, refused.Index, cell.refuseAt)
			}
			if cell.forbidden != "" && strings.Contains(refused.Message, cell.forbidden) {
				t.Errorf("%s: the refusal %q names the submitted value", cell.name, refused.Message)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: err=%v, want accepted", cell.name, err)
			continue
		}
		if change.Clear != cell.clear || !slices.Equal(change.Prefixes, cell.want) {
			t.Errorf("%s: change=%+v, want clear=%v prefixes=%v", cell.name, change, cell.clear, cell.want)
		}
	}
}

func TestArtifactExcludeParseRefusesEveryNonGitHubProvider(t *testing.T) {
	value := listOf("ci-images-")
	for _, provider := range []string{"github", "GitHub"} {
		if _, err := Parse(provider, value); err != nil {
			t.Errorf("%s: err=%v, want accepted", provider, err)
		}
	}
	for _, provider := range []string{"gitlab", "jira", "linear", "launchdarkly", "pagerduty", ""} {
		_, err := Parse(provider, value)
		var refused *Refusal
		if !errors.As(err, &refused) {
			t.Errorf("%q: err=%v, want a refusal: the field does nothing outside the GitHub tests sync", provider, err)
			continue
		}
		if strings.Contains(refused.Message, "ci-images-") {
			t.Errorf("%q: the refusal %q names the submitted value", provider, refused.Message)
		}
	}
}

func TestArtifactExcludeParseDecodedMatchesParse(t *testing.T) {
	for _, cell := range []struct {
		decoded any
		value   pyjson.Value
	}{
		{decoded: []any{"ci-images-", "ci-part-"}, value: listOf("ci-images-", "ci-part-")},
		{decoded: nil, value: nil},
		{decoded: []any{}, value: listOf()},
		{decoded: []any{""}, value: listOf("")},
		{decoded: []any{"a b"}, value: listOf("a b")},
		{decoded: []any{float64(3)}, value: listOf(pyjson.IntOf(3))},
		{decoded: "ci-images-", value: "ci-images-"},
	} {
		fromDecoded, errDecoded := ParseDecoded(cell.decoded)
		fromAPI, errAPI := Parse("github", cell.value)
		if (errDecoded == nil) != (errAPI == nil) || fromDecoded.Clear != fromAPI.Clear || !slices.Equal(fromDecoded.Prefixes, fromAPI.Prefixes) {
			t.Errorf("%#v: decoded=(%+v, %v) api=(%+v, %v), want the same answer", cell.decoded, fromDecoded, errDecoded, fromAPI, errAPI)
		}
	}
}

func TestArtifactExcludeMatchesIsACaseInsensitivePrefix(t *testing.T) {
	prefixes := []string{"ci-images-", "CI-Timings-"}
	for name, want := range map[string]bool{
		"ci-images-amd64-1": true, "CI-IMAGES-x": true, "ci-timings-3": true, "ci-images": false, "test-results": false, "x-ci-images-1": false,
	} {
		if got := Matches(name, prefixes); got != want {
			t.Errorf("Matches(%q)=%v, want %v", name, got, want)
		}
	}
	if Matches("anything", nil) {
		t.Error("an empty list matched a name")
	}
}

func TestArtifactExcludeApplyTouchesOnlyItsKey(t *testing.T) {
	stored := `{"fetch_comments": true, "comments_limit": 500, "ci_artifact_exclude_prefixes": ["old-"], "all_repos": true}`
	text, changed, err := apply(&stored, Change{Prefixes: []string{"ci-images-"}})
	if err != nil || !changed {
		t.Fatalf("apply: changed=%v err=%v", changed, err)
	}
	if want := `{"fetch_comments": true, "comments_limit": 500, "ci_artifact_exclude_prefixes": ["ci-images-"], "all_repos": true}`; text != want {
		t.Fatalf("set: %s, want %s (the key keeps its place, no other key moves)", text, want)
	}
	text, changed, err = apply(&text, Change{Clear: true})
	if err != nil || !changed {
		t.Fatalf("clear: changed=%v err=%v", changed, err)
	}
	if want := `{"fetch_comments": true, "comments_limit": 500, "all_repos": true}`; text != want {
		t.Fatalf("clear: %s, want %s", text, want)
	}
	again, changed, err := apply(&text, Change{Clear: true})
	if err != nil || changed || again != text {
		t.Fatalf("a second clear: changed=%v err=%v text=%s, want no change", changed, err, again)
	}
	added, changed, err := apply(&text, Change{Prefixes: []string{"ci-images-"}})
	if err != nil || !changed || !strings.HasSuffix(added, `"all_repos": true, "ci_artifact_exclude_prefixes": ["ci-images-"]}`) {
		t.Fatalf("add: changed=%v err=%v text=%s, want the key appended", changed, err, added)
	}
	same, changed, err := apply(&added, Change{Prefixes: []string{"ci-images-"}})
	if err != nil || changed || same != added {
		t.Fatalf("the same list again: changed=%v err=%v, want no change", changed, err)
	}
}
