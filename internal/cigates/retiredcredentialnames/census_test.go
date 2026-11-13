// Package retiredcredentialnames holds the census of the names of retired credentials and the retired legacy ingest:
// the svc_acr_, svc_authsess_ and svc_worker_ service credentials and their class ids, go-api's session issuer token
// AUTH_SESSION_ISSUE_TOKEN, and the legacy ingest package and settings. No tracked file may name one of them outside
// the exact-path allowlist below.
//
// The retired service-credential table, its verbs, CLI and store have their own census (retiredservicecred). The
// HS256 names are listed in waiting: they are retired only when HS256 is (CHAOS-10457), so this census does not
// enforce them yet.
package retiredcredentialnames

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
	nameSvcAcr            = "svc_acr"
	nameSvcAuthsess       = "svc_authsess"
	nameSvcWorker         = "svc_worker"
	nameSessionIssueToken = "session_issue_token"
	nameLegacyIngest      = "legacy_ingest"
	nameClassAcr          = "class_acr"
	nameClassAuthsess     = "class_authsess"
	nameClassWorker       = "class_worker"
)

// retiredNames are the retired names. A prefix pattern starts at a word boundary, so a class id such as
// internal_svc_acr_token does not count as the svc_acr_ prefix; the class ids have their own names.
var retiredNames = map[string]*regexp.Regexp{
	nameSvcAcr:            regexp.MustCompile(`\bsvc_acr_`),
	nameSvcAuthsess:       regexp.MustCompile(`\bsvc_authsess_`),
	nameSvcWorker:         regexp.MustCompile(`\bsvc_worker_`),
	nameSessionIssueToken: regexp.MustCompile(`\bAUTH_SESSION_ISSUE_TOKEN\b`),
	nameLegacyIngest:      regexp.MustCompile(`internal/api/legacyingest|\bDEV_HEALTH_LEGACY_INGEST_[A-Z_]*`),
	nameClassAcr:          regexp.MustCompile(`\binternal_svc_acr_token\b`),
	nameClassAuthsess:     regexp.MustCompile(`\binternal_svc_authsess_token\b`),
	nameClassWorker:       regexp.MustCompile(`\binternal_svc_worker_token\b`),
}

// waiting are retired names this census does not enforce yet, each with the ticket that retires it. Once that ticket
// lands, a name moves to retiredNames with its allowlist rows.
var waiting = map[string]string{
	"JWT_SECRET_KEY":         "CHAOS-10457",
	"legacy_hs256":           "CHAOS-10457",
	"ops_access_token_hs256": "CHAOS-10457",
	"GO_API_EDGE_JWT_SECRET": "CHAOS-10457",
}

const (
	// history: migration history, retired evidence, refusal and ban cells, and decision records; kept.
	history = "history"
	// untilDrop: names a live svc_ credential row; the drop of the service credential table removes the name.
	untilDrop = "until the drop"
	// sweep: stale configuration, code or text the sweep removes (go-api's session issuer token mode and its tests).
	sweep = "sweep"
)

type allowance struct {
	names []string
	stage string
}

// allowed is the exact-path allowlist: each path may name only its listed retired names.
var allowed = map[string]allowance{
	".github/docs-legacy/architecture/acr-entitlement-route.md":                                               {[]string{nameSvcAcr}, history},
	"ci/auth_corpus_external.sh":                                                                              {[]string{nameSvcAuthsess}, history},
	"ci/auth_rotation_probe_test.sh":                                                                          {[]string{nameSvcAuthsess}, history},
	"ci/rehearse_auth_key_rotation_k3s_test.sh":                                                               {[]string{nameSvcAuthsess}, history},
	"ci/rehearse_auth_key_rotation_test.sh":                                                                   {[]string{nameSvcAuthsess}, history},
	"contracts/auth/v1/README.md":                                                                             {[]string{nameClassAuthsess}, history},
	"contracts/auth/v1/credential-classes.json":                                                               {[]string{nameSvcAcr, nameSvcAuthsess, nameSvcWorker, nameClassAcr, nameClassAuthsess, nameClassWorker}, history},
	"contracts/auth/v1/endpoint-profiles.auth-service.json":                                                   {[]string{nameSvcAuthsess}, history},
	"contracts/auth/v1/endpoint-profiles.go-api.json":                                                         {[]string{nameSvcAcr}, history},
	"contracts/auth/v1/jsonschema/credential-entry.v1.schema.json":                                            {[]string{nameClassAcr, nameClassAuthsess, nameClassWorker}, history},
	"contracts/auth/v1/jsonschema/credential-list.v1.schema.json":                                             {[]string{nameClassAcr, nameClassAuthsess, nameClassWorker}, history},
	"contracts/auth/v1/jsonschema/credential-receipt.v1.schema.json":                                          {[]string{nameClassAcr, nameClassAuthsess, nameClassWorker}, history},
	"contracts/auth/v1/jsonschema/principal.v1.schema.json":                                                   {[]string{nameClassAcr, nameClassAuthsess, nameClassWorker}, history},
	"deploy/helm/dev-health/go_api_legacy_ingest_retired_test.go":                                             {[]string{nameLegacyIngest}, history},
	"deploy/helm/dev-health/templates/go-api-deployment.yaml":                                                 {[]string{nameLegacyIngest}, history},
	"docs-data/auth/credential-reference.json":                                                                {[]string{nameClassAcr, nameClassAuthsess, nameClassWorker}, history},
	"docs/contribute/architecture/auth-control-plane-adrs.md":                                                 {[]string{nameSvcAcr, nameSvcWorker}, history},
	"docs/operate/runbooks/auth-service-accounts.md":                                                          {[]string{nameSvcAuthsess}, history},
	"docs/operate/runbooks/auth-users-owned-flip-and-rollback.md":                                             {[]string{nameSvcAuthsess}, history},
	"docs/operate/runbooks/legacy-ingest-retirement.md":                                                       {[]string{nameLegacyIngest}, history},
	"docs/operate/security/acr-service-credential-migration.md":                                               {[]string{nameSvcAcr}, history},
	"docs/reference/auth/acr-credential-authority.md":                                                         {[]string{nameSvcAcr}, history},
	"docs/reference/auth/billing-and-worker-boundaries.md":                                                    {[]string{nameSvcWorker}, history},
	"docs/reference/auth/configuration.md":                                                                    {[]string{nameSvcAuthsess, nameSessionIssueToken}, history},
	"docs/reference/auth/credential-classes.md":                                                               {[]string{nameSvcAcr, nameSvcAuthsess, nameSvcWorker, nameLegacyIngest, nameClassAcr, nameClassAuthsess, nameClassWorker}, history},
	"docs/reference/auth/credential-reference.md":                                                             {[]string{nameSvcAcr, nameSvcAuthsess, nameSvcWorker, nameClassAcr, nameClassAuthsess, nameClassWorker}, history},
	"docs/reference/auth/endpoint-profiles.md":                                                                {[]string{nameClassWorker}, history},
	"docs/reference/cli/index.md":                                                                             {[]string{nameSvcAcr, nameSvcAuthsess, nameSvcWorker}, history},
	"internal/adminops/orgprovisioning_test.go":                                                               {[]string{nameSvcAuthsess, nameSessionIssueToken}, history},
	"internal/api/authprovision/workload_credential_test.go":                                                  {[]string{nameSvcAuthsess}, history},
	"internal/api/externalingest/refusal_audit_test.go":                                                       {[]string{nameSvcWorker}, history},
	"internal/api/policy/principalonce.go":                                                                    {[]string{nameClassAcr, nameClassAuthsess, nameClassWorker}, history},
	"internal/api/sessionissue/workload_issue_test.go":                                                        {[]string{nameSvcAuthsess}, history},
	"internal/api/sessionissue/workload_state_test.go":                                                        {[]string{nameSvcAuthsess}, history},
	"internal/apiservice/acr/access_test.go":                                                                  {[]string{nameSvcAcr}, history},
	"internal/apiservice/acr/credentials_integration_test.go":                                                 {[]string{nameSvcAcr}, untilDrop},
	"internal/apiservice/acr/handlers.go":                                                                     {[]string{nameSvcAcr}, history},
	"internal/apiservice/acr/internalauth.go":                                                                 {[]string{nameSvcAcr}, history},
	"internal/apiservice/acr/internalauth_test.go":                                                            {[]string{nameSvcAcr, nameSvcWorker}, history},
	"internal/apiservice/acr/llmconfig_test.go":                                                               {[]string{nameSvcAcr}, history},
	"internal/apiservice/acr/user_tokens_refused_test.go":                                                     {[]string{nameSvcAcr}, history},
	"internal/apiservice/acr/workload_audit_integration_test.go":                                              {[]string{nameSvcAcr}, untilDrop},
	"internal/apiservice/acr/workloadauth_size_test.go":                                                       {[]string{nameSvcAcr}, history},
	"internal/apiservice/acr/workloadauth_test.go":                                                            {[]string{nameSvcAcr, nameSvcAuthsess, nameSvcWorker}, history},
	"internal/apiservice/admin_users_rows_cells_test.go":                                                      {[]string{nameSvcAcr}, history},
	"internal/apiservice/externalingest_wrong_class_matrix_test.go":                                           {[]string{nameClassAcr, nameClassAuthsess, nameClassWorker}, history},
	"internal/apiservice/legacyingest_retired_test.go":                                                        {[]string{nameLegacyIngest}, history},
	"internal/apiservice/provisioning_credential_test.go":                                                     {[]string{nameSvcAuthsess, nameSessionIssueToken}, history},
	"internal/apiservice/session_issuer_test.go":                                                              {[]string{nameSvcAuthsess, nameSessionIssueToken}, history},
	"internal/apiservice/session_workload_credential_test.go":                                                 {[]string{nameSvcAuthsess, nameSessionIssueToken}, history},
	"internal/apiservice/venue_seed_integration_test.go":                                                      {[]string{nameSvcAcr}, untilDrop},
	"internal/audit/allowlist_test.go":                                                                        {[]string{nameSvcWorker}, history},
	"internal/auth/authapi/acr_events_test.go":                                                                {[]string{nameSvcAuthsess, nameClassAuthsess}, history},
	"internal/auth/authapi/authorize_auth_test.go":                                                            {[]string{nameSvcAcr, nameSvcAuthsess}, history},
	"internal/auth/authapi/authorize_test.go":                                                                 {[]string{nameSvcAuthsess}, history},
	"internal/auth/authapi/issue_workload.go":                                                                 {[]string{nameSvcAuthsess}, history},
	"internal/auth/authapi/issue_workload_integration_test.go":                                                {[]string{nameSvcAuthsess}, history},
	"internal/auth/authapi/issue_workload_test.go":                                                            {[]string{nameSvcAcr, nameSvcAuthsess, nameSvcWorker}, history},
	"internal/auth/authapi/provisioning_read_rows_test.go":                                                    {[]string{nameSvcAuthsess}, history},
	"internal/auth/authapi/provisioning_rows_test.go":                                                         {[]string{nameSvcAuthsess, nameClassAuthsess}, history},
	"internal/auth/authapi/provisioning_test.go":                                                              {[]string{nameSvcAuthsess}, history},
	"internal/auth/authapi/session_retired_credential_test.go":                                                {[]string{nameSvcAuthsess}, history},
	"internal/auth/authapi/session_state_test.go":                                                             {[]string{nameClassAuthsess}, history},
	"internal/auth/authruntime/issue_session_integration_test.go":                                             {[]string{nameSvcAuthsess}, history},
	"internal/auth/contracts/credential_class_svc_retired_test.go":                                            {[]string{nameSvcAcr, nameSvcAuthsess, nameSvcWorker, nameClassAcr, nameClassAuthsess, nameClassWorker}, history},
	"internal/auth/contracts/credentialanchors/credential-anchor-retired-evidence.tsv":                        {[]string{nameSvcAcr, nameClassAcr, nameClassWorker}, history},
	"internal/auth/contracts/endpoint_profiles_go_api_admin_fields_test.go":                                   {[]string{nameSvcAcr, nameClassAcr}, history},
	"internal/auth/contracts/endpoint_profiles_provisioning_workload_test.go":                                 {[]string{nameClassAuthsess}, history},
	"internal/auth/contracts/endpoint_profiles_registry_reconcile_test.go":                                    {[]string{nameClassAuthsess}, history},
	"internal/auth/contracts/endpoint_profiles_session_issue_workload_test.go":                                {[]string{nameSvcAuthsess, nameClassAuthsess}, history},
	"internal/auth/contracts/fixture_secret_scanner_test.go":                                                  {[]string{nameSvcAcr, nameSvcWorker}, history},
	"internal/auth/contracts/read_api_test.go":                                                                {[]string{nameClassAuthsess}, history},
	"internal/auth/contracts/tokencorpus/service_account_test.go":                                             {[]string{nameSvcAcr, nameSvcWorker}, history},
	"internal/auth/contracts/tokencorpus/workload_test.go":                                                    {[]string{nameSvcAcr, nameSvcWorker}, history},
	"internal/auth/workload/bootstrap_integration_test.go":                                                    {[]string{nameClassAuthsess}, history},
	"internal/auth/workload/bootstrap_test.go":                                                                {[]string{nameSvcAuthsess, nameClassAuthsess}, history},
	"internal/auth/workloadexchange/bootstrap_test.go":                                                        {[]string{nameSvcAuthsess}, history},
	"internal/cigates/bigboy/compose_acr_venue_test.go":                                                       {[]string{nameSvcAcr}, history},
	"internal/cigates/queuecreds/scan_test.go":                                                                {[]string{nameSvcWorker}, history},
	"internal/cigates/scripts/auth_workload_session_issue_k3s_test.go":                                        {[]string{nameSvcAuthsess}, history},
	"internal/cigates/workflows/auth_corpus_external_behaviour_test.go":                                       {[]string{nameSvcAuthsess, nameSessionIssueToken}, history},
	"internal/cigates/workflows/live_e2e_auth_stack_test.go":                                                  {[]string{nameSessionIssueToken}, history},
	"internal/pgmigrate/archive/sql/0157_add_internal_service_credential_audits_principal_id.sql":             {[]string{nameSvcAcr}, history},
	"internal/pgmigrate/internal_audit_principal_integration_test.go":                                         {[]string{nameSvcAcr}, untilDrop},
	"internal/storage/postgres/api_authorization.go":                                                          {[]string{nameSvcAcr}, untilDrop},
	"internal/storage/postgres/authschema/additive_integration_test.go":                                       {[]string{nameClassAcr, nameClassAuthsess, nameClassWorker}, history},
	"internal/storage/postgres/authschema/migration_0017_integration_test.go":                                 {[]string{nameClassAcr, nameClassAuthsess}, history},
	"internal/storage/postgres/authschema/migration_0022_integration_test.go":                                 {[]string{nameClassAuthsess}, history},
	"internal/storage/postgres/authschema/migrations/0008_credential_registry_wire_shapes.sql":                {[]string{nameClassAcr, nameClassWorker}, history},
	"internal/storage/postgres/authschema/migrations/0017_credential_class_internal_svc_authsess.sql":         {[]string{nameSvcAuthsess, nameClassAcr, nameClassAuthsess, nameClassWorker}, history},
	"internal/storage/postgres/authschema/migrations/0022_credential_class_link_credential.sql":               {[]string{nameClassAcr, nameClassAuthsess, nameClassWorker}, history},
	"internal/storage/postgres/authschema/migrations/0023_credential_class_pagerduty_webhook_signature.sql":   {[]string{nameClassAcr, nameClassAuthsess, nameClassWorker}, history},
	"internal/storage/postgres/authschema/migrations/0030_credential_class_kubernetes_sa_token_exchange.sql":  {[]string{nameClassAcr, nameClassAuthsess, nameClassWorker}, history},
	"internal/storage/postgres/authschema/migrations/0032_credential_class_workload_bootstrap_credential.sql": {[]string{nameClassAcr, nameClassAuthsess, nameClassWorker}, history},
	"internal/storage/postgres/authschema/migrations/0036_credential_class_retire_legacy_ingest.sql":          {[]string{nameClassAcr, nameClassAuthsess, nameClassWorker}, history},
	"internal/storage/postgres/authschema/migrations/0037_credential_class_acr_browser_refresh_token.sql":     {[]string{nameClassAcr, nameClassAuthsess, nameClassWorker}, history},
	"internal/storage/postgres/authschema/migrations/0040_credential_class_service_account_key.sql":           {[]string{nameClassAcr, nameClassAuthsess, nameClassWorker}, history},
	"internal/storage/postgres/authschema/migrations/0050_credential_class_service_account_token.sql":         {[]string{nameClassAcr, nameClassAuthsess, nameClassWorker}, history},
	"internal/storage/postgres/authschema/migrations/0055_credential_class_oauth_authorization.sql":           {[]string{nameClassAcr, nameClassAuthsess, nameClassWorker}, history},
	"internal/storage/postgres/domain_authorization.go":                                                       {[]string{nameSvcWorker}, untilDrop},
	"internal/testsupport/recordedfiles/beforestamp_dayone_test.go":                                           {[]string{nameLegacyIngest}, history},
	"internal/cigates/retiredcredentialnames/census_test.go":                                                  {[]string{nameSvcAcr, nameSvcAuthsess, nameSvcWorker, nameSessionIssueToken, nameLegacyIngest, nameClassAcr, nameClassAuthsess, nameClassWorker}, history},
	"internal/cigates/retiredcredentialnames/planted_test.go":                                                 {[]string{nameSvcAcr, nameSvcAuthsess, nameSvcWorker, nameSessionIssueToken, nameLegacyIngest, nameClassAcr, nameClassAuthsess, nameClassWorker}, history},
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
var literals = [][]byte{
	[]byte("svc_acr_"), []byte("svc_authsess_"), []byte("svc_worker_"), []byte("AUTH_SESSION_ISSUE_TOKEN"),
	[]byte("internal/api/legacyingest"), []byte("DEV_HEALTH_LEGACY_INGEST_"),
	[]byte("internal_svc_acr_token"), []byte("internal_svc_authsess_token"), []byte("internal_svc_worker_token"),
}

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

func TestNoReferenceToTheRetiredCredentialNames(t *testing.T) {
	hits := census(t, repoRoot(t), allowed)
	if len(hits) == 0 {
		return
	}
	lines := make([]string, len(hits))
	for i, h := range hits {
		lines[i] = "  " + h.String()
	}
	t.Errorf("these credential names are retired; %d reference(s) outside the allowlist:\n%s", len(hits), strings.Join(lines, "\n"))
}

// staleEntries lists every allowlist entry that is not a tracked file, has no known stage, names an unknown name, or no
// longer holds a name it may hold: an entry outlives its reason only by failing here, so the list shrinks with the code.
func staleEntries(t *testing.T, root string, allow map[string]allowance) []string {
	t.Helper()
	tracked := map[string]bool{}
	for _, rel := range trackedFiles(t, root) {
		tracked[rel] = true
	}
	var stale []string
	for rel, a := range allow {
		if !tracked[rel] {
			stale = append(stale, rel+": not a tracked file")
			continue
		}
		if a.stage != history && a.stage != untilDrop && a.stage != sweep {
			stale = append(stale, rel+": stage "+strconv.Quote(a.stage))
		}
		body, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(rel)))
		if err != nil {
			t.Fatalf("read %s: %v", rel, err)
		}
		for _, name := range a.names {
			re, ok := retiredNames[name]
			if !ok {
				stale = append(stale, rel+": unknown name "+strconv.Quote(name))
				continue
			}
			if !re.Match(body) && !re.MatchString(rel) {
				stale = append(stale, rel+": no longer holds the "+name+" name")
			}
		}
	}
	sort.Strings(stale)
	return stale
}

func TestEveryAllowlistEntryIsStillNeeded(t *testing.T) {
	for _, s := range staleEntries(t, repoRoot(t), allowed) {
		t.Errorf("allowlist entry %s: drop it from the entry", s)
	}
}

// A waiting name is never also enforced, and each names the ticket that retires it.
func TestWaitingNamesAreApartAndNameTheirTicket(t *testing.T) {
	ticket := regexp.MustCompile(`^CHAOS-[0-9]+$`)
	for name, blocker := range waiting {
		if !ticket.MatchString(blocker) {
			t.Errorf("waiting name %s: blocker %q is not a ticket id", name, blocker)
		}
		for key, re := range retiredNames {
			if re.MatchString(name) {
				t.Errorf("waiting name %s is also matched by the enforced name %s", name, key)
			}
		}
	}
}
