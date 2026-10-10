package daily

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Team = ownership of repositories and projects only. No daily metric family
// resolves a team through teams.members (the roster column of a team). A new
// family that resolves a team by that roster fails here.
//
// The ic_finalize family was the one exception. It no longer reads the
// roster: it takes the teams of a PERSON, for the person's own row and
// landscape points, from the team_memberships rows valid at the day.
func TestNoDailyFamilyResolvesATeamThroughTeamMembers(t *testing.T) {
	allowed := map[string]string{
		"wellbeing_native_clickhouse.go": "defines the member resolver and the team read",
	}
	files, err := filepath.Glob("*.go")
	if err != nil || len(files) == 0 {
		t.Fatalf("no source file read (err %v): a census that read nothing must fail", err)
	}
	scanned := 0
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		scanned++
		if _, ok := allowed[file]; ok {
			continue
		}
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		for _, needle := range []string{"NewMemberResolver(", "memberResolver.ResolveMember(", ".ResolveMember("} {
			if strings.Contains(string(raw), needle) {
				t.Errorf("%s resolves a team through teams.members (%s): Team is ownership only, see CHAOS-9084", file, needle)
			}
		}
	}
	if scanned < 20 {
		t.Fatalf("census scanned %d files: the directory was not read", scanned)
	}
}
