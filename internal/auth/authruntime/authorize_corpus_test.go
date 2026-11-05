package authruntime

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

// The golden decision corpus replayed through the production router
// (authorize_corpus_integration_test.go): every pdp row is replayed or named
// here with a reason from a closed set. The pdp runner
// (internal/auth/authorization) keeps its own no-skip rule; a row here is
// state the router cannot reach, not a skipped verdict.

type azExclusion string

const (
	// A workload principal reference does not resolve: decisionstate reads no
	// workload limits (Workload is always absent).
	azWorkloadPrincipalUnreachable azExclusion = "workload_principal_unreachable"
	// The replay seeds no service-account principal, key or grant row. decisionstate's
	// service-account read is replayed against a real schema in its own integration cells.
	azServiceAccountUnreachable azExclusion = "service_account_unreachable"
	// A nil snapshot, a grant read failure or a missing clock is a 503 on the
	// router, never a decision.
	azStateFailureIs503 azExclusion = "state_failure_is_503"
	// A status PostgreSQL cannot hold (a CHECK constraint or a boolean column).
	azStatusNotRepresentable azExclusion = "status_not_representable"
	// decisionstate always returns a grant snapshot.
	azPlainSnapshotUnreachable azExclusion = "plain_snapshot_unreachable"
	// decisionstate builds the membership from the pair it read: its org id is
	// always the asked organization, so org_mismatch on a membership is unreachable.
	azMembershipOrgIsTheKey azExclusion = "membership_org_is_the_key"
	// The router denies every delegated session delegation_invalid until the
	// decision core decides delegation on this route.
	azDelegatedSessionPending azExclusion = "delegated_session_pending"
)

var azExclusionReasons = []azExclusion{
	azWorkloadPrincipalUnreachable, azServiceAccountUnreachable, azStateFailureIs503, azStatusNotRepresentable,
	azPlainSnapshotUnreachable, azMembershipOrgIsTheKey, azDelegatedSessionPending,
}

// A reason that rests on how the real stack is built names the cell that pins
// that construction. The cell is a test function in the named file; dropping it,
// or dropping the entry, fails TestAuthorizeCorpusExclusionPinCellsExist.
type azPinCell struct{ file, fn string }

var azExclusionPinCells = map[azExclusion]azPinCell{
	azMembershipOrgIsTheKey:     {"../decisionstate/decisionstate_integration_test.go", "TestSnapshotMembershipOrgIsTheAskedOrg"},
	azServiceAccountUnreachable: {"../decisionstate/decisionstate_serviceaccount_integration_test.go", "TestServiceAccountSnapshotGrantStates"},
}

var azCorpusExclusions = map[string]azExclusion{
	"workload-action-outside-limits":   azWorkloadPrincipalUnreachable,
	"workload-audience-empty":          azWorkloadPrincipalUnreachable,
	"workload-audience-not-registered": azWorkloadPrincipalUnreachable,
	"workload-in-limits-allowed":       azWorkloadPrincipalUnreachable,
	"workload-inactive-principal":      azWorkloadPrincipalUnreachable,
	"workload-no-allowed-actions":      azWorkloadPrincipalUnreachable,

	"org-action-service-account-is-no-membership": azServiceAccountUnreachable,
	"repo-service-account-expired-grant":          azServiceAccountUnreachable,
	"repo-service-account-inactive":               azServiceAccountUnreachable,
	"repo-service-account-live-grant":             azServiceAccountUnreachable,
	"repo-service-account-org-mismatch":           azServiceAccountUnreachable,
	"repo-service-account-revoked-grant":          azServiceAccountUnreachable,
	"repo-service-account-without-grants":         azServiceAccountUnreachable,

	"nil-snapshot":           azStateFailureIs503,
	"repo-grant-read-failed": azStateFailureIs503,
	"repo-clock-unavailable": azStateFailureIs503,

	"membership-active-uppercase":       azStatusNotRepresentable,
	"principal-status-uppercase-active": azStatusNotRepresentable,

	"repo-plain-snapshot-has-no-grants":  azPlainSnapshotUnreachable,
	"org-filtered-plain-snapshot-denies": azPlainSnapshotUnreachable,

	"cross-org-membership-row-for-other-org": azMembershipOrgIsTheKey,
	"repo-membership-org-mismatch":           azMembershipOrgIsTheKey,

	"delegated-owner-manages-members-is-impersonation-denied":          azDelegatedSessionPending,
	"delegated-request-for-another-organization-is-delegation-invalid": azDelegatedSessionPending,
}

// azMalformedAtTheRouter are the replayed rows whose rendered body the
// authorize decoder refuses: 400 malformed before any state read. The decision
// core denies each with invalid_request.
var azMalformedAtTheRouter = []string{
	"non-canonical-trailing-newline-action",
	"non-canonical-uppercase-action",
	"non-canonical-uppercase-repo-key",
	"non-canonical-wildcard-on-organization",
	"unknown-resource-type-team",
}

const azCorpusDir = "contracts/auth/v1/golden/decision"

type azManifest struct {
	Description string            `json:"description"`
	Surface     string            `json:"surface"`
	Catalogs    []json.RawMessage `json:"catalogs"`
	Vectors     []struct {
		File  string `json:"file"`
		ID    string `json:"id"`
		Layer string `json:"layer"`
	} `json:"vectors"`
}

type azGrant struct {
	OrgID        string  `json:"org_id"`
	Role         string  `json:"role"`
	ResourceKind string  `json:"resource_kind"`
	ResourceKey  string  `json:"resource_key"`
	RepoID       string  `json:"resource_repo_id"`
	ExpiresAt    *string `json:"expires_at"`
	Revoked      bool    `json:"revoked"`
}

type azMembership struct {
	OrgID  string `json:"org_id"`
	Role   string `json:"role"`
	Status string `json:"status"`
}

type azState struct {
	Catalog         string                     `json:"catalog"`
	Snapshot        string                     `json:"snapshot"`
	Clock           *string                    `json:"clock"`
	PrincipalStatus string                     `json:"principal_status"`
	PlatformRole    string                     `json:"platform_role"`
	Organizations   map[string]string          `json:"organizations"`
	Memberships     map[string]azMembership    `json:"memberships"`
	ServiceAccounts map[string]json.RawMessage `json:"service_accounts"`
	Grants          []azGrant                  `json:"grants"`
	GrantsError     bool                       `json:"grants_error"`
	Workload        json.RawMessage            `json:"workload"`
	Delegation      json.RawMessage            `json:"delegation"`
}

type azRequest struct {
	Action         string `json:"action"`
	ResourceKind   string `json:"resource_kind"`
	OrganizationID string `json:"organization_id"`
	ResourceKey    string `json:"resource_key"`
}

type azExpect struct {
	Allowed        bool   `json:"allowed"`
	Reason         string `json:"reason"`
	CacheClass     string `json:"cache_class"`
	ExpiresIn      *int64 `json:"expires_in_seconds"`
	PolicyRevision int    `json:"policy_revision"`
	// WireReasonCode, when a row carries it, is compared instead of Reason.
	WireReasonCode *string `json:"wire_reason_code"`
	// Obligations is the decision's obligations in wire form, or null.
	Obligations json.RawMessage `json:"obligations"`
}

type azVector struct {
	ID      string    `json:"id"`
	Layer   string    `json:"layer"`
	Why     string    `json:"why"`
	State   azState   `json:"state"`
	Request azRequest `json:"request"`
	Expect  azExpect  `json:"expect"`
}

func (e azExpect) wireReason() string {
	if e.WireReasonCode != nil {
		return *e.WireReasonCode
	}
	return e.Reason
}

func azStrict(t *testing.T, path string, into any) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(into); err != nil {
		t.Fatalf("%s: %v", path, err)
	}
}

// azCorpus reads the manifest's layer of every row and every pdp row, in
// manifest order.
func azCorpus(t *testing.T) (map[string]string, []azVector) {
	t.Helper()
	root, err := contracts.RepoRoot(".")
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(root, azCorpusDir)
	var m azManifest
	azStrict(t, filepath.Join(dir, "manifest.json"), &m)
	layers := map[string]string{}
	var pdp []azVector
	for _, row := range m.Vectors {
		layers[row.ID] = row.Layer
		if row.Layer != "pdp" {
			continue
		}
		var v azVector
		azStrict(t, filepath.Join(dir, "vectors", row.File), &v)
		if v.ID != row.ID || v.Layer != "pdp" {
			t.Fatalf("manifest row %s: file says id %q layer %q", row.ID, v.ID, v.Layer)
		}
		pdp = append(pdp, v)
	}
	if len(pdp) == 0 {
		t.Fatal("the corpus holds no pdp row")
	}
	return layers, pdp
}

// azReplayedRows are the pdp rows the router replays: every row the exclusion
// table does not name, each one the seeder can represent.
func azReplayedRows(t *testing.T) []azVector {
	t.Helper()
	_, pdp := azCorpus(t)
	var out []azVector
	for _, v := range pdp {
		if _, excluded := azCorpusExclusions[v.ID]; excluded {
			continue
		}
		if why, err := azBlocker(v); err != nil || why != "" {
			t.Fatalf("%s: not in the exclusion table but not replayable (%q, %v)", v.ID, why, err)
		}
		out = append(out, v)
	}
	return out
}

func azPresent(raw json.RawMessage) bool { return len(raw) > 0 && string(raw) != "null" }

var (
	azPrincipalStatuses    = []string{"active", "suspended"}
	azOrganizationStatuses = []string{"active", "suspended"}
	azMembershipStatuses   = []string{"active", "invited", "suspended", "removed"}
	// auth.resource_grants.role_key references auth.roles; the replay seeds
	// platform_admin there (a role the catalog scopes to the platform).
	azGrantRoles = []string{"viewer", "member", "admin", "platform_admin"}
)

// azBlocker is the exclusion reason of a row the router cannot replay, "" for
// a row it replays. An error is a row shape outside what this file knows: the
// guard fails until the row is named.
func azBlocker(v azVector) (azExclusion, error) {
	s := v.State
	switch {
	case azPresent(s.Delegation):
		return azDelegatedSessionPending, nil
	case azPresent(s.Workload):
		return azWorkloadPrincipalUnreachable, nil
	case len(s.ServiceAccounts) > 0:
		return azServiceAccountUnreachable, nil
	case s.Snapshot == "nil" || s.GrantsError || s.Clock == nil:
		return azStateFailureIs503, nil
	case s.Snapshot == "plain":
		return azPlainSnapshotUnreachable, nil
	case s.Snapshot != "grant":
		return "", fmt.Errorf("snapshot %q is outside the closed set", s.Snapshot)
	case s.Catalog != "embedded":
		return "", fmt.Errorf("catalog %q is outside the closed set", s.Catalog)
	case s.PlatformRole != "" && s.PlatformRole != "platform_admin":
		return "", fmt.Errorf("platform role %q has no users row form", s.PlatformRole)
	}
	if _, err := time.Parse(time.RFC3339, *s.Clock); err != nil {
		return "", fmt.Errorf("clock: %w", err)
	}
	if !slices.Contains(azPrincipalStatuses, s.PrincipalStatus) {
		return azStatusNotRepresentable, nil
	}
	for _, label := range azSortedKeys(s.Organizations) {
		if !slices.Contains(azOrganizationStatuses, s.Organizations[label]) {
			return azStatusNotRepresentable, nil
		}
	}
	for _, key := range azSortedKeys(s.Memberships) {
		m := s.Memberships[key]
		if m.OrgID != key {
			return azMembershipOrgIsTheKey, nil
		}
		if !slices.Contains(azMembershipStatuses, m.Status) {
			return azStatusNotRepresentable, nil
		}
		if m.Status != "active" {
			return azStatusNotRepresentable, nil
		}
	}
	for i, g := range s.Grants {
		if _, ok := s.Organizations[g.OrgID]; !ok {
			return "", fmt.Errorf("grant %d: organization %q is not projected", i, g.OrgID)
		}
		if g.ResourceKind != "repository" || !slices.Contains(azGrantRoles, g.Role) {
			return "", fmt.Errorf("grant %d: kind %q role %q have no row form", i, g.ResourceKind, g.Role)
		}
		if g.ExpiresAt != nil {
			if _, err := time.Parse(time.RFC3339, *g.ExpiresAt); err != nil {
				return "", fmt.Errorf("grant %d: expires_at: %w", i, err)
			}
		}
	}
	orgs := map[string]string{}
	for _, label := range azOrgLabels(s) {
		orgs[label] = uuid.NewString()
	}
	if _, err := azRenderOrg(v.Request.OrganizationID, orgs); err != nil {
		return "", err
	}
	return "", nil
}

func azSortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// azOrgLabels are the synthetic organization ids a row's state names.
func azOrgLabels(s azState) []string {
	set := map[string]bool{}
	for label := range s.Organizations {
		set[label] = true
	}
	for label := range s.Memberships {
		set[label] = true
	}
	for _, g := range s.Grants {
		set[g.OrgID] = true
	}
	return azSortedKeys(set)
}

// azRenderOrg maps a request's synthetic organization id to the seeded uuid. A
// non-canonical form (uppercase, a space for the first hyphen) gets the same
// transform on the uuid.
func azRenderOrg(asked string, orgs map[string]string) (string, error) {
	if asked == "" {
		return "", nil
	}
	if id, ok := orgs[asked]; ok {
		return id, nil
	}
	for _, label := range azSortedKeys(orgs) {
		id := orgs[label]
		switch asked {
		case strings.ToUpper(label):
			return strings.ToUpper(id), nil
		case strings.Replace(label, "-", " ", 1):
			return strings.Replace(id, "-", " ", 1), nil
		}
	}
	return "", fmt.Errorf("request organization %q maps to no state organization", asked)
}

// azRequestID is a fixed authorization_request.v1 context request id.
const azRequestID = "req_AZCORPUSREPLAY000000000000"

type azResourceDoc struct {
	Type           string `json:"type"`
	Key            string `json:"key"`
	OrganizationID string `json:"organization_id,omitempty"`
}

type azContextDoc struct {
	RequestID     string `json:"request_id"`
	ClientService string `json:"client_service"`
}

type azRequestDoc struct {
	SchemaVersion      string        `json:"schema_version"`
	PrincipalReference string        `json:"principal_reference"`
	Action             string        `json:"action"`
	Resource           azResourceDoc `json:"resource"`
	Context            azContextDoc  `json:"context"`
}

// azRenderBody renders a row's request as authorization_request.v1. The PDP
// reads no resource key outside a repository: an empty one becomes the
// resource type, which the wire pattern takes.
func azRenderBody(v azVector, orgs map[string]string, reference string) ([]byte, string, error) {
	org, err := azRenderOrg(v.Request.OrganizationID, orgs)
	if err != nil {
		return nil, "", err
	}
	key := v.Request.ResourceKey
	if key == "" && v.Request.ResourceKind != "repository" {
		key = v.Request.ResourceKind
	}
	body, err := json.Marshal(azRequestDoc{
		SchemaVersion: "authorization_request.v1", PrincipalReference: reference, Action: v.Request.Action,
		Resource: azResourceDoc{Type: v.Request.ResourceKind, Key: key, OrganizationID: org},
		Context:  azContextDoc{RequestID: azRequestID, ClientService: "dev-health-web"},
	})
	return body, org, err
}

func azBatchBody(items [][]byte) ([]byte, error) {
	requests := make([]json.RawMessage, len(items))
	for i, item := range items {
		requests[i] = item
	}
	return json.Marshal(struct {
		SchemaVersion string            `json:"schema_version"`
		Requests      []json.RawMessage `json:"requests"`
	}{"authorization_batch_request.v1", requests})
}

func TestAuthorizeCorpusEveryPDPRowIsReplayedOrExcluded(t *testing.T) {
	_, pdp := azCorpus(t)
	replayed := 0
	for _, v := range pdp {
		why, err := azBlocker(v)
		if err != nil {
			t.Errorf("%s: %v", v.ID, err)
			continue
		}
		if want := azCorpusExclusions[v.ID]; why != want {
			t.Errorf("%s: the router replay finds %q, the exclusion table says %q", v.ID, why, want)
		}
		if why == "" {
			replayed++
		}
	}
	if replayed == 0 {
		t.Fatal("no pdp row is replayed")
	}
	t.Logf("pdp rows %d: replayed %d, excluded %d", len(pdp), replayed, len(azCorpusExclusions))
}

func TestAuthorizeCorpusExclusionsAreClosedAndExist(t *testing.T) {
	layers, pdp := azCorpus(t)
	byID := map[string]azVector{}
	for _, v := range pdp {
		byID[v.ID] = v
	}
	used := map[azExclusion]int{}
	for id, why := range azCorpusExclusions {
		if layers[id] != "pdp" {
			t.Errorf("excluded row %s is not a pdp row of the manifest (layer %q)", id, layers[id])
		}
		if !slices.Contains(azExclusionReasons, why) {
			t.Errorf("excluded row %s: reason %q is outside the closed set", id, why)
		}
		used[why]++
	}
	for i, why := range azExclusionReasons {
		if slices.Contains(azExclusionReasons[:i], why) {
			t.Errorf("reason %q is listed twice", why)
		}
		if used[why] == 0 {
			t.Errorf("reason %q excludes no row", why)
		}
	}
	for i, id := range azMalformedAtTheRouter {
		v, ok := byID[id]
		switch {
		case !ok:
			t.Errorf("malformed row %s is not a pdp row", id)
		case slices.Contains(azMalformedAtTheRouter[:i], id):
			t.Errorf("malformed row %s is listed twice", id)
		case azCorpusExclusions[id] != "":
			t.Errorf("malformed row %s is also excluded", id)
		case v.Expect.Allowed || v.Expect.Reason != "invalid_request":
			t.Errorf("malformed row %s: the decision core says %v %s, want a deny invalid_request", id, v.Expect.Allowed, v.Expect.Reason)
		}
	}
}

func TestAuthorizeCorpusRenderedBodiesDecodeAsExpected(t *testing.T) {
	reference := "az-reference-not-a-token-0000000000"
	var accepted [][]byte
	for _, v := range azReplayedRows(t) {
		orgs := map[string]string{}
		for _, label := range azOrgLabels(v.State) {
			orgs[label] = uuid.NewString()
		}
		body, org, err := azRenderBody(v, orgs, reference)
		if err != nil {
			t.Errorf("%s: render: %v", v.ID, err)
			continue
		}
		q, err := authapi.DecodeAuthorizationRequest(body)
		if refused, want := err != nil, slices.Contains(azMalformedAtTheRouter, v.ID); refused != want {
			t.Errorf("%s: decoder refused %v (%v), want refused %v; body %s", v.ID, refused, err, want, body)
			continue
		}
		if err != nil {
			continue
		}
		if q.Action != v.Request.Action || q.ResourceType != v.Request.ResourceKind || q.OrganizationID != org ||
			(q.ResourceType == "repository" && q.ResourceKey != v.Request.ResourceKey) || q.PrincipalReference != reference {
			t.Errorf("%s: decoded %+v from %s", v.ID, q, body)
		}
		accepted = append(accepted, body)
	}
	batch, err := azBatchBody(accepted)
	if err != nil {
		t.Fatal(err)
	}
	questions, err := authapi.DecodeAuthorizationBatchRequest(batch)
	if err != nil || len(questions) != len(accepted) {
		t.Fatalf("one batch of the %d accepted rows: %d questions, %v", len(accepted), len(questions), err)
	}
}

func TestAuthorizeCorpusExclusionPinCellsExist(t *testing.T) {
	if _, ok := azExclusionPinCells[azMembershipOrgIsTheKey]; !ok {
		t.Fatalf("reason %q names no pin cell", azMembershipOrgIsTheKey)
	}
	for why, cell := range azExclusionPinCells {
		if !slices.Contains(azExclusionReasons, why) {
			t.Errorf("pin cell for %q: the reason is outside the closed set", why)
		}
		file, err := parser.ParseFile(token.NewFileSet(), cell.file, nil, 0)
		if err != nil {
			t.Errorf("reason %q: pin cell file %s: %v", why, cell.file, err)
			continue
		}
		found := false
		for _, d := range file.Decls {
			if f, ok := d.(*ast.FuncDecl); ok && f.Recv == nil && f.Name.Name == cell.fn && strings.HasPrefix(f.Name.Name, "Test") {
				found = true
			}
		}
		if !found {
			t.Errorf("reason %q: pin cell %s is not a test function of %s", why, cell.fn, cell.file)
		}
	}
}
