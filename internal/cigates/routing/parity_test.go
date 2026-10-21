package routing_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cigates/routing"
)

// recordedDeploySHA is the deploy commit the recording built (gitEnv makes it the same everywhere).
const recordedDeploySHA = "66f8e3b6fb34ea33c2b031d319ffa6e60ef1f428"

func readParity(t *testing.T, dir, rel string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, rel))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestCLIOutputParityRecordedPair replays each pair the Python tools recorded at the main tip (testdata/README.md):
// the same arguments on the same inputs give the same exit code, stdout, stderr and written files, byte for byte.
func TestCLIOutputParityRecordedPair(t *testing.T) {
	tools := map[string]func([]string, routing.Deps, *bytes.Buffer, *bytes.Buffer) int{
		"plane-split-router-pinned": gen,
		"plane-split-router-labels": gen,
		"query-api-flags":           flagsTool,
		"route-coverage":            coverage,
		"web-env-parity":            webEnv,
	}
	dir := filepath.Join(routingRepoRoot(t), "internal", "cigates", "routing", "testdata", "parity")
	for _, pair := range []string{"plane-split-router-pinned", "plane-split-router-labels", "query-api-flags", "route-coverage", "web-env-parity"} {
		t.Run(pair, func(t *testing.T) {
			work := t.TempDir()
			for _, in := range []string{"values.prod.yaml", "compose.bigboy.yml", "web-env-names.txt"} {
				writeFile(t, work, in, readParity(t, dir, in))
			}
			if err := os.Mkdir(filepath.Join(work, pair), 0o755); err != nil {
				t.Fatal(err)
			}
			subst := map[string]string{"{VALUES_OUT}": filepath.Join(work, "values-out.yaml")}
			if pair == "plane-split-router-pinned" {
				repo, sha := deployRepo(t, readParity(t, dir, "values.prod.yaml"), "0123456789abcdef0123456789abcdef01234567")
				if sha != recordedDeploySHA {
					t.Fatalf("the deploy commit is %s, the recording's is %s: the replay would not be the recorded run", sha, recordedDeploySHA)
				}
				subst["{DEPLOY_REPO}"] = repo
			}
			if pair == "route-coverage" {
				var statuses map[string]map[string]int
				if err := json.Unmarshal(readParity(t, dir, "route-statuses.json"), &statuses); err != nil {
					t.Fatal(err)
				}
				for key, plane := range map[string]string{"{GO_URL}": "go", "{QUERY_URL}": "query"} {
					table := statuses[plane]
					srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						status, ok := table[r.RequestURI]
						if !ok {
							status = 405
						}
						if r.Method != "ROUTEPROBE" {
							status = 400
						}
						w.WriteHeader(status)
					}))
					t.Cleanup(srv.Close)
					subst[key] = srv.URL
				}
			}
			var args []string
			for _, a := range strings.Split(strings.TrimSuffix(string(readParity(t, dir, pair+"/args")), "\n"), "\n") {
				if v, ok := subst[a]; ok {
					a = v
				}
				args = append(args, a)
			}
			t.Chdir(work)
			var so, se bytes.Buffer
			code := tools[pair](args, routing.OSDeps(), &so, &se)
			if want := strings.TrimSpace(string(readParity(t, dir, pair+"/rc"))); strconv.Itoa(code) != want {
				t.Errorf("exit %d, recorded %s", code, want)
			}
			if want := string(readParity(t, dir, pair+"/stdout")); so.String() != want {
				t.Errorf("stdout differs from the recording\ngot:\n%s\nrecorded:\n%s", so.String(), want)
			}
			if want := string(readParity(t, dir, pair+"/stderr")); se.String() != want {
				t.Errorf("stderr differs from the recording\ngot:\n%s\nrecorded:\n%s", se.String(), want)
			}
			written := map[string]string{
				"plane-split-router-pinned": filepath.Join(work, "values-out.yaml"),
				"query-api-flags":           filepath.Join(work, "query-api-flags", "query-api-flags.out"),
			}
			recordedName := map[string]string{
				"plane-split-router-pinned": "plane-split-router-pinned/values-out.yaml",
				"query-api-flags":           "query-api-flags/query-api-flags.out",
			}
			if path, ok := written[pair]; ok {
				got, err := os.ReadFile(path)
				if err != nil {
					t.Fatalf("the tool wrote no %s: %v", path, err)
				}
				if want := readParity(t, dir, recordedName[pair]); !bytes.Equal(got, want) {
					t.Errorf("%s differs from the recording\ngot:\n%s\nrecorded:\n%s", path, got, want)
				}
			}
		})
	}
}
