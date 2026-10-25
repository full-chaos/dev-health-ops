package registerorg

import (
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestEnabledIsGoAPIsParseOverEveryValue(t *testing.T) {
	cases := map[string]bool{
		"": true, " ": true, "1": true, "true": true, "True": true, "yes": true, "on": true, "junk": true, "2": true,
		"0": false, "false": false, "FALSE": false, "False": false, "no": false, "No": false, "off": false, "OFF": false,
		" off ": false, "\toff\n": false, " off ": false, " 0 ": false,
		"offf": true, "of f": true, "falsey": true,
	}
	for raw, want := range cases {
		if got := Enabled(raw); got != want {
			t.Errorf("Enabled(%q) = %v, want %v", raw, got, want)
		}
	}
}

func TestNameAndSlugFollowGoAPIsRule(t *testing.T) {
	id := uuid.MustParse("0a1b2c3d-1111-2222-3333-444455556666")
	empty := ""
	given := "Acme Corp"
	if Name(nil) != DefaultName || Name(&empty) != DefaultName || Name(&given) != given {
		t.Fatal("Name does not fall back to the default for a missing or empty value")
	}
	if got := Slug("Acme Corp", id); got != "acme-corp-0a1b2c3d" {
		t.Fatalf("slug = %q", got)
	}
	long := strings.Repeat("É", 60)
	got := Slug(long, id)
	if want := strings.Repeat("é", 50) + "-0a1b2c3d"; got != want {
		t.Fatalf("long slug = %q, want 50 code points then the user id prefix", got)
	}
}
