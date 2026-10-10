package daily

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// CHAOS-9084 class: Team = ownership of repositories and projects only. No
// daily metric family resolves a team through teams.members (a person's
// membership), except the ic_finalize family, which is the open exception of
// CHAOS-9084 and is NOT part of this change. A new family that resolves a team
// by membership fails here.
func TestNoDailyFamilyResolvesATeamThroughTeamMembersExceptICFinalize(t *testing.T) {
	allowed := map[string]string{
		"wellbeing_native_clickhouse.go": "defines the member resolver and the team read",
		"ic_finalize_native_executor.go": "CHAOS-9084: the open exception, IC landscape",
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
	icFinalize, err := os.ReadFile("ic_finalize_native_executor.go")
	if err != nil || !strings.Contains(string(icFinalize), "NewMemberResolver(") {
		t.Errorf("the named exception no longer uses the member resolver: remove it from the census (err %v)", err)
	}
}
