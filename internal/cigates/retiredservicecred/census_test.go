// Package retiredservicecred holds the census of the retired service credentials: the
// public.internal_service_credentials table, the `dho service-credentials` verbs, the servicecredcli package and the
// internal/auth/servicecred store. No tracked file may name one of them outside the exact-path allowlist below.
package retiredservicecred

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

const (
	nameTable = "table"
	nameVerbs = "verbs"
	nameCLI   = "cli"
	nameStore = "store"
)

// retiredNames are the retired names. The table pattern is a whole word, so internal_service_credential_audits (kept)
// does not match it.
var retiredNames = map[string]*regexp.Regexp{
	nameTable: regexp.MustCompile(`\binternal_service_credentials\b`),
	nameVerbs: regexp.MustCompile(`\bservice-credentials\b`),
	nameCLI:   regexp.MustCompile(`\bservicecredcli\b|internal__servicecredcli\b`),
	nameStore: regexp.MustCompile(`internal/auth/servicecred\b|internal__auth__servicecred\b`),
}

const (
	// history: migration history, retired evidence, ban lists and decision records; kept after the drop.
	history = "history"
	// untilDrop: the table still exists and these files name it; the drop migration removes the name from each.
	untilDrop = "until the drop"
)

type allowance struct {
	names []string
	stage string
}

// allowed is the exact-path allowlist: each path may name only its listed retired names.
var allowed = map[string]allowance{
	".github/docs-legacy/architecture/acr-entitlement-route.md":                                  {[]string{nameTable}, history},
	".github/docs-legacy/architecture/chaos-3033-coordinator-pool-activation.md":                 {[]string{nameTable}, history},
	".github/docs-legacy/architecture/chaos-3033-role-partition-manifest.md":                     {[]string{nameTable}, history},
	".github/docs-legacy/ops/workers.md":                                                         {[]string{nameVerbs}, history},
	"cmd/dho/retired_verbs_test.go":                                                              {[]string{nameVerbs}, history},
	"contracts/auth/v1/credential-classes.json":                                                  {[]string{nameTable, nameVerbs}, history},
	"contracts/auth/v1/credential-classes.schema.json":                                           {[]string{nameTable}, untilDrop},
	"docs-data/auth/credential-reference.json":                                                   {[]string{nameTable}, untilDrop},
	"docs/go-migration-matrix.md":                                                                {[]string{nameVerbs}, history},
	"docs/operate/runbooks/operator-commands.md":                                                 {[]string{nameTable, nameVerbs}, untilDrop},
	"docs/operate/security/acr-service-credential-migration.md":                                  {[]string{nameVerbs}, history},
	"docs/reference/auth/acr-credential-authority.md":                                            {[]string{nameVerbs}, history},
	"docs/reference/auth/billing-and-worker-boundaries.md":                                       {[]string{nameVerbs}, history},
	"docs/reference/auth/configuration.md":                                                       {[]string{nameTable, nameVerbs}, untilDrop},
	"docs/reference/auth/credential-classes.md":                                                  {[]string{nameTable, nameVerbs}, untilDrop},
	"docs/reference/auth/credential-reference.md":                                                {[]string{nameTable}, untilDrop},
	"docs/reference/auth/endpoint-profiles.md":                                                   {[]string{nameVerbs}, history},
	"docs/reference/cli/index.md":                                                                {[]string{nameTable, nameVerbs}, untilDrop},
	"internal/apiservice/acr/credentials_integration_test.go":                                    {[]string{nameTable}, untilDrop},
	"internal/apiservice/acr/workload_audit_integration_test.go":                                 {[]string{nameTable}, untilDrop},
	"internal/apiservice/venue_seed_integration_test.go":                                         {[]string{nameTable}, untilDrop},
	"internal/auth/authruntime/issue_session_integration_test.go":                                {[]string{nameTable, nameVerbs}, untilDrop},
	"internal/auth/restorefence/inventory_completeness_test.go":                                  {[]string{nameTable}, untilDrop},
	"internal/auth/restorefence/narrowed_scopes_test.go":                                         {[]string{nameTable}, history},
	"internal/auth/restorefence/specs_test.go":                                                   {[]string{nameTable}, history},
	"internal/auth/writerinventory/scan_test.go":                                                 {[]string{nameTable}, untilDrop},
	"internal/cigates/webproxyproof/proof_test.go":                                               {[]string{nameTable}, history},
	"internal/cli/testdata/golden/root_flags.json":                                               {[]string{nameVerbs}, history},
	"internal/joboperator/postgres_integration_test.go":                                          {[]string{nameTable}, untilDrop},
	"internal/pgmigrate/archive/baseline-0138/head.json":                                         {[]string{nameTable}, history},
	"internal/pgmigrate/baseline/head.json":                                                      {[]string{nameTable}, history},
	"internal/pgmigrate/internal_audit_principal_integration_test.go":                            {[]string{nameTable}, untilDrop},
	"internal/pgmigrate/testdata/hand_ddl_ledger.tsv":                                            {[]string{nameTable}, untilDrop},
	"internal/pushcli/testdata/golden/push.json":                                                 {[]string{nameVerbs}, history},
	"internal/storage/postgres/api_authorization.go":                                             {[]string{nameTable, nameVerbs}, untilDrop},
	"internal/storage/postgres/authschema/public_reads_integration_test.go":                      {[]string{nameTable}, untilDrop},
	"internal/storage/postgres/authschema/publicreads.go":                                        {[]string{nameTable}, untilDrop},
	"internal/storage/postgres/authschema/retired_credentials_grant_revoked_test.go":             {[]string{nameTable}, untilDrop},
	"internal/storage/postgres/authschema/retired_credentials_grant_upgrade_integration_test.go": {[]string{nameTable}, untilDrop},
	"internal/storage/postgres/coordinator_statement_privileges_integration_test.go":             {[]string{nameTable}, untilDrop},
	"internal/storage/postgres/domain_authorization.go":                                          {[]string{nameTable}, untilDrop},
	"internal/storage/postgres/retired_table_posture_test.go":                                    {[]string{nameTable}, untilDrop},
	"internal/cigates/retiredservicecred/census_test.go":                                         {[]string{nameTable, nameVerbs, nameCLI, nameStore}, history},
	"internal/cigates/retiredservicecred/planted_test.go":                                        {[]string{nameTable, nameVerbs, nameCLI, nameStore}, history},
}

type hit struct {
	path, where, name string
}

func (h hit) String() string { return h.path + ":" + h.where + ": " + h.name }

// trackedFiles is `git ls-files -z` in root. A git failure fails the test: a census that scanned nothing would pass.
func trackedFiles(t *testing.T, root string) []string {
	t.Helper()
	cmd := exec.Command("git", "-c", "maintenance.auto=false", "-c", "gc.auto=0", "-C", root, "ls-files", "-z")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("git ls-files in %s: %v: %s", root, err, stderr.String())
	}
	var files []string
	for _, rel := range strings.Split(string(out), "\x00") {
		if rel != "" {
			files = append(files, rel)
		}
	}
	if len(files) == 0 {
		t.Fatalf("git ls-files in %s listed no file", root)
	}
	return files
}

// census returns every retired name a tracked file holds in its path or text that its allowlist entry does not
// permit. Binary files (a NUL byte) are scanned by path only.
func census(t *testing.T, root string, allow map[string]allowance) []hit {
	t.Helper()
	var hits []hit
	for _, rel := range trackedFiles(t, root) {
		permitted := map[string]bool{}
		if a, ok := allow[rel]; ok {
			for _, name := range a.names {
				permitted[name] = true
			}
		}
		hits = append(hits, match(rel, "path", rel, permitted)...)
		body, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(rel)))
		if os.IsNotExist(err) {
			continue // tracked but deleted in the work tree
		}
		if err != nil {
			t.Fatalf("read %s: %v", rel, err)
		}
		if bytes.IndexByte(body, 0) >= 0 || !mayHold(body) {
			continue
		}
		for i, line := range strings.Split(string(body), "\n") {
			hits = append(hits, match(rel, strconv.Itoa(i+1), line, permitted)...)
		}
	}
	sort.Slice(hits, func(i, j int) bool { return hits[i].String() < hits[j].String() })
	return hits
}

// literals holds one literal each retired pattern contains, so a file holding none of them is skipped unsplit.
var literals = [][]byte{[]byte("internal_service_credentials"), []byte("service-credentials"), []byte("servicecred")}

func mayHold(body []byte) bool {
	for _, l := range literals {
		if bytes.Contains(body, l) {
			return true
		}
	}
	return false
}

func match(rel, where, text string, permitted map[string]bool) []hit {
	var hits []hit
	for _, name := range sortedNames() {
		if !permitted[name] && retiredNames[name].MatchString(text) {
			hits = append(hits, hit{rel, where, name})
		}
	}
	return hits
}

func sortedNames() []string {
	names := make([]string, 0, len(retiredNames))
	for name := range retiredNames {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func repoRoot(t *testing.T) string {
	t.Helper()
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func TestNoReferenceToTheRetiredServiceCredentials(t *testing.T) {
	hits := census(t, repoRoot(t), allowed)
	if len(hits) == 0 {
		return
	}
	lines := make([]string, len(hits))
	for i, h := range hits {
		lines[i] = "  " + h.String()
	}
	t.Errorf("the service credentials are retired; %d reference(s) outside the allowlist:\n%s", len(hits), strings.Join(lines, "\n"))
}

// Every allowlist entry is a tracked file that still holds each name it may hold: an entry outlives its reason only
// by failing here, so the list shrinks with the code.
func TestEveryAllowlistEntryIsStillNeeded(t *testing.T) {
	root := repoRoot(t)
	tracked := map[string]bool{}
	for _, rel := range trackedFiles(t, root) {
		tracked[rel] = true
	}
	for rel, a := range allowed {
		if !tracked[rel] {
			t.Errorf("allowlist entry %s is not a tracked file", rel)
			continue
		}
		if a.stage != history && a.stage != untilDrop {
			t.Errorf("allowlist entry %s has stage %q", rel, a.stage)
		}
		body, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(rel)))
		if err != nil {
			t.Fatalf("read %s: %v", rel, err)
		}
		for _, name := range a.names {
			re, ok := retiredNames[name]
			if !ok {
				t.Errorf("allowlist entry %s names an unknown name %q", rel, name)
				continue
			}
			if !re.Match(body) && !re.MatchString(rel) {
				t.Errorf("allowlist entry %s no longer holds the %s name: drop it from the entry", rel, name)
			}
		}
	}
}
