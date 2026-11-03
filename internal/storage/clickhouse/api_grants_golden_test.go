package clickhouse

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The bigboy users.d declaration of dho_api_ch (`cigate bigboy render-ch-users`) is rendered by
// Python from this same manifest. Two implementations of one grant list drift unless one file pins both:
// this test pins GrantStatements(APIPosture) to testdata/api_grants.golden and the Python renderer's
// test (internal/cigates/bigboy/compose_ch_users_test.go) pins its output to the SAME file.
//
// The golden lists each statement as users.d holds it, without the "TO <role>" suffix (the declaration
// already names the user). To regenerate after a deliberate manifest change: set UPDATE_API_GRANTS_GOLDEN=1
// and run this test once, then review the diff.
const apiGrantsGoldenRole = "dho_api_ch"

func TestAPIGrantStatementsMatchTheGoldenTheBigboyRendererIsPinnedTo(t *testing.T) {
	var got []string
	suffix := " TO " + apiGrantsGoldenRole
	for _, statement := range GrantStatements(apiGrantsGoldenRole, APIPosture("default")) {
		if !strings.HasSuffix(statement, suffix) {
			t.Fatalf("statement %q does not end with %q", statement, suffix)
		}
		got = append(got, strings.TrimSuffix(statement, suffix))
	}
	if len(got) == 0 {
		t.Fatal("APIPosture produced no grants: this test would prove nothing")
	}
	rendered := strings.Join(got, "\n") + "\n"
	path := filepath.Join("testdata", "api_grants.golden")
	if os.Getenv("UPDATE_API_GRANTS_GOLDEN") == "1" {
		if err := os.WriteFile(path, []byte(rendered), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(want) != rendered {
		t.Fatalf("GrantStatements(APIPosture) no longer matches %s; if the manifest change is deliberate, regenerate it (UPDATE_API_GRANTS_GOLDEN=1 go test -run TestAPIGrantStatementsMatch ./internal/storage/clickhouse) and re-run internal/cigates/bigboy (TestComposeCHRendererReal)\n--- golden\n%s--- now\n%s", path, want, rendered)
	}
}

// With ACR_ACCESS_PROJECT_SCOPE=true go-api requires APIPostureFor(db, true) (CHAOS-12224). The bigboy renderer's
// `--project-scope` render is pinned to this golden (internal/cigates/bigboyrender, CHAOS-12268), so the users.d
// file of a flag-on go-api and the posture its readiness check requires cannot drift apart. It must be the flag-off
// golden plus exactly the ProjectScopeGrant line, at the end. Regenerate it with UPDATE_API_GRANTS_GOLDEN=1, as above.
func TestAPIGrantStatementsForTheProjectScopeMatchTheirGolden(t *testing.T) {
	suffix := " TO " + apiGrantsGoldenRole
	statements := func(posture Posture) []string {
		var out []string
		for _, statement := range GrantStatements(apiGrantsGoldenRole, posture) {
			if !strings.HasSuffix(statement, suffix) {
				t.Fatalf("statement %q does not end with %q", statement, suffix)
			}
			out = append(out, strings.TrimSuffix(statement, suffix))
		}
		return out
	}
	off, on := statements(APIPostureFor("default", false)), statements(APIPostureFor("default", true))
	if len(off) == 0 {
		t.Fatal("APIPostureFor(false) produced no grants: this test would prove nothing")
	}
	if len(on) != len(off)+1 || strings.Join(on[:len(off)], "\n") != strings.Join(off, "\n") || on[len(off)] != "GRANT SELECT ON default.projects" {
		t.Fatalf("the flag-on statements are not the flag-off ones plus GRANT SELECT ON default.projects at the end:\n%s", strings.Join(on, "\n"))
	}
	rendered := strings.Join(on, "\n") + "\n"
	path := filepath.Join("testdata", "api_grants_project_scope.golden")
	if os.Getenv("UPDATE_API_GRANTS_GOLDEN") == "1" {
		if err := os.WriteFile(path, []byte(rendered), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(want) != rendered {
		t.Fatalf("GrantStatements(APIPostureFor(db, true)) no longer matches %s\n--- golden\n%s--- now\n%s", path, want, rendered)
	}
}
