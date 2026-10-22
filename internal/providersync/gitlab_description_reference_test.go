package providersync

import (
	"testing"
	"time"
)

func TestGitLabDescriptionReferencesUseTheOneParser(t *testing.T) {
	t.Parallel()
	claim := nativeTestClaim("gitlab", "work-items")
	for name, tc := range map[string]struct {
		description string
		wantTarget  string
		wantRaw     string
	}{
		"dotted project path": {"see group/my.project#3", "gitlab:group/my.project#3", "explicit_text"},
		"same project":        {"see #3", "gitlab:acme/api#3", "explicit_text"},
		"blocks keyword":      {"this blocks group/my.project#3", "gitlab:group/my.project#3", "explicit_text:blocks"},
		"blocking keyword":    {"this is blocking #3", "gitlab:acme/api#3", "explicit_text:blocking"},
		"blocked by keyword":  {"blocked by #3", "gitlab:acme/api#3", "explicit_text:blocked by"},
		"iid past 32 bits":    {"see #4294967296", "", ""},
		"leading zero iid":    {"see #012", "", ""},
		"leading slash path":  {"see /#4", "", ""},
		"empty path segment":  {"see a//b#4", "", ""},
		"dot segment":         {"see a/../b#4", "", ""},
		"zero iid":            {"see #0", "", ""},
	} {
		t.Run(name, func(t *testing.T) {
			rows := normalizeGitLabDependencies(claim, "gitlab:acme/api#5", "acme/api", tc.description, nil, time.Now())
			if tc.wantTarget == "" {
				if len(rows) != 0 {
					t.Fatalf("%q gave rows %+v, want none", tc.description, rows)
				}
				return
			}
			if len(rows) != 1 || rows[0].TargetWorkItemID != tc.wantTarget && rows[0].SourceWorkItemID != tc.wantTarget ||
				rows[0].RelationshipTypeRaw != tc.wantRaw {
				t.Fatalf("%q gave %+v, want the item %q with raw %q", tc.description, rows, tc.wantTarget, tc.wantRaw)
			}
			// acr relationcurrent.go:48-65 reads the raw only when relation_writer is empty.
			if rows[0].RelationWriter == nil || *rows[0].RelationWriter == "" {
				t.Fatalf("%q gave a text row with no relation_writer: %+v", tc.description, rows[0])
			}
		})
	}
}

func TestGitLabDescriptionKeywordRawDiffersFromTheNativeLinkKind(t *testing.T) {
	t.Parallel()
	claim := nativeTestClaim("gitlab", "work-items")
	rows := normalizeGitLabDependencies(claim, "gitlab:acme/api#5", "acme/api", "this blocks #7", nil, time.Now())
	if len(rows) != 1 || rows[0].RelationshipTypeRaw == "blocks" {
		t.Fatalf("a text keyword row must not carry the native raw kind: %+v", rows)
	}
}
