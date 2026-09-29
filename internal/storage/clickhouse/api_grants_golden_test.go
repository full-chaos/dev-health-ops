package clickhouse

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The bigboy users.d declaration of dho_api_ch (ci/bigboy/render-dho-api-ch-users.py) is rendered by
// Python from this same manifest. Two implementations of one grant list drift unless one file pins both:
// this test pins GrantStatements(APIPosture) to testdata/api_grants.golden and the Python renderer's
// test (tests/tooling/test_bigboy_clickhouse_users.py) pins its output to the SAME file.
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
		t.Fatalf("GrantStatements(APIPosture) no longer matches %s; if the manifest change is deliberate, regenerate it (UPDATE_API_GRANTS_GOLDEN=1 go test -run TestAPIGrantStatementsMatch ./internal/storage/clickhouse) and re-run tests/tooling/test_bigboy_clickhouse_users.py\n--- golden\n%s--- now\n%s", path, want, rendered)
	}
}
