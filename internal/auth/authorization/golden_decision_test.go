package authorization

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
)

const goldenDecisionDir = "contracts/auth/v1/golden/decision"

type goldenManifest struct {
	Description string     `json:"description"`
	Surface     string     `json:"surface"`
	Catalogs    []struct{} `json:"catalogs"`
	Vectors     []struct {
		File  string `json:"file"`
		ID    string `json:"id"`
		Layer string `json:"layer"`
	} `json:"vectors"`
}

type goldenGrant struct {
	OrgID        string  `json:"org_id"`
	Role         string  `json:"role"`
	ResourceKind string  `json:"resource_kind"`
	ResourceKey  string  `json:"resource_key"`
	ExpiresAt    *string `json:"expires_at"`
	Revoked      bool    `json:"revoked"`
}

type goldenMembership struct {
	OrgID  string `json:"org_id"`
	Role   string `json:"role"`
	Status string `json:"status"`
}

type goldenServiceAccount struct {
	OrgID  string `json:"org_id"`
	Status string `json:"status"`
}

type goldenWorkload struct {
	Audience       string   `json:"audience"`
	Audiences      []string `json:"audiences"`
	AllowedActions []string `json:"allowed_actions"`
}

type goldenState struct {
	Catalog         string                          `json:"catalog"`
	Snapshot        string                          `json:"snapshot"`
	Clock           *string                         `json:"clock"`
	PrincipalStatus string                          `json:"principal_status"`
	PlatformRole    string                          `json:"platform_role"`
	Organizations   map[string]string               `json:"organizations"`
	Memberships     map[string]goldenMembership     `json:"memberships"`
	ServiceAccounts map[string]goldenServiceAccount `json:"service_accounts"`
	Grants          []goldenGrant                   `json:"grants"`
	GrantsError     bool                            `json:"grants_error"`
	Workload        *goldenWorkload                 `json:"workload"`
}

type goldenRequest struct {
	Action         string `json:"action"`
	ResourceKind   string `json:"resource_kind"`
	OrganizationID string `json:"organization_id"`
	ResourceKey    string `json:"resource_key"`
}

type goldenExpect struct {
	Allowed        bool   `json:"allowed"`
	Reason         string `json:"reason"`
	CacheClass     string `json:"cache_class"`
	ExpiresIn      *int64 `json:"expires_in_seconds"`
	PolicyRevision int    `json:"policy_revision"`
}

type goldenVector struct {
	ID      string        `json:"id"`
	Layer   string        `json:"layer"`
	Why     string        `json:"why"`
	State   goldenState   `json:"state"`
	Request goldenRequest `json:"request"`
	Expect  goldenExpect  `json:"expect"`
}

func goldenRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("repository root not found")
		}
		dir = parent
	}
}

func goldenStrict(t *testing.T, path string, into any) {
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

func goldenParseTime(t *testing.T, s string) int64 {
	t.Helper()
	ts, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatalf("time %q: %v", s, err)
	}
	return ts.UnixNano()
}

type goldenSnap struct {
	st       goldenState
	now      int64
	grants   []Grant
	workload *WorkloadLimits
}

func (g goldenSnap) PrincipalStatus() Status { return ParseStatus(g.st.PrincipalStatus) }
func (g goldenSnap) PlatformRole() string    { return g.st.PlatformRole }
func (g goldenSnap) OrganizationStatus(org string) Status {
	return ParseStatus(g.st.Organizations[org])
}
func (g goldenSnap) Membership(org string) (Membership, bool) {
	m, ok := g.st.Memberships[org]
	return Membership{OrgID: m.OrgID, Role: m.Role, Status: ParseStatus(m.Status)}, ok
}

type goldenGrantSnap struct{ goldenSnap }

func (g goldenGrantSnap) Grants(string) ([]Grant, error) {
	if g.st.GrantsError {
		return nil, os.ErrInvalid
	}
	return g.grants, nil
}
func (g goldenGrantSnap) Workload() (WorkloadLimits, bool) {
	if g.workload == nil {
		return WorkloadLimits{}, false
	}
	return *g.workload, true
}
func (g goldenGrantSnap) ServiceAccount(org string) (ServiceAccount, bool) {
	sa, ok := g.st.ServiceAccounts[org]
	return ServiceAccount{OrgID: sa.OrgID, Status: ParseStatus(sa.Status)}, ok
}
func (g goldenGrantSnap) NowUnixNano() int64 { return g.now }

func goldenEvaluator(t *testing.T, name string) *Evaluator {
	t.Helper()
	if name != "embedded" {
		t.Fatalf("catalog %q is outside the closed set", name)
	}
	return NewEvaluator()
}

func goldenSnapshot(t *testing.T, st goldenState) Snapshot {
	t.Helper()
	var now int64
	if st.Clock != nil {
		now = goldenParseTime(t, *st.Clock)
	}
	base := goldenSnap{st: st, now: now}
	for _, g := range st.Grants {
		row := Grant{OrgID: g.OrgID, Role: g.Role, ResourceKind: g.ResourceKind, ResourceKey: g.ResourceKey, Revoked: g.Revoked}
		if g.ExpiresAt != nil {
			row.ExpiresAtUnixNano = goldenParseTime(t, *g.ExpiresAt)
		}
		base.grants = append(base.grants, row)
	}
	if st.Workload != nil {
		base.workload = &WorkloadLimits{Audience: st.Workload.Audience, Audiences: st.Workload.Audiences, AllowedActions: st.Workload.AllowedActions}
	}
	switch st.Snapshot {
	case "grant":
		return goldenGrantSnap{base}
	case "plain":
		return base
	case "nil":
		return nil
	}
	t.Fatalf("snapshot %q is outside the closed set", st.Snapshot)
	return nil
}

func TestGoldenDecisionPDPRowsReachTheirVerdict(t *testing.T) {
	root := goldenRoot(t)
	dir := filepath.Join(root, "contracts/auth/v1/golden/decision")
	var m goldenManifest
	goldenStrict(t, filepath.Join(dir, "manifest.json"), &m)
	want := 0
	for _, r := range m.Vectors {
		switch r.Layer {
		case "pdp":
			want++
		case "pep":
		default:
			t.Errorf("row %s: layer %q is outside the closed set pdp|pep", r.ID, r.Layer)
		}
	}
	if want == 0 {
		t.Fatal("the manifest holds no pdp row; the runner would pass vacuously")
	}
	ran := 0
	reasons := map[Reason]int{}
	for _, r := range m.Vectors {
		if r.Layer != "pdp" {
			continue
		}
		var v goldenVector
		goldenStrict(t, filepath.Join(dir, "vectors", r.File), &v)
		if v.Layer != "pdp" || v.ID != r.ID {
			t.Errorf("row %s: file says layer %q id %q", r.ID, v.Layer, v.ID)
			continue
		}
		ran++
		e := goldenEvaluator(t, v.State.Catalog)
		d := e.Evaluate(Request{Action: v.Request.Action, ResourceKind: v.Request.ResourceKind,
			OrganizationID: v.Request.OrganizationID, ResourceKey: v.Request.ResourceKey}, goldenSnapshot(t, v.State))
		reasons[d.Reason]++
		if d.Allowed != v.Expect.Allowed {
			t.Errorf("%s: allowed %v, want %v (%s)", v.ID, d.Allowed, v.Expect.Allowed, d.Reason)
		}
		if string(d.Reason) != v.Expect.Reason {
			t.Errorf("%s: reason %q, want %q", v.ID, d.Reason, v.Expect.Reason)
		}
		if string(d.CacheClass) != v.Expect.CacheClass {
			t.Errorf("%s: cache_class %q, want %q", v.ID, d.CacheClass, v.Expect.CacheClass)
		}
		if d.PolicyRevision != v.Expect.PolicyRevision {
			t.Errorf("%s: policy_revision %d, want %d", v.ID, d.PolicyRevision, v.Expect.PolicyRevision)
		}
		switch {
		case v.Expect.ExpiresIn == nil && d.ExpiresAtUnixNano != 0:
			t.Errorf("%s: expires_at %d, want none", v.ID, d.ExpiresAtUnixNano)
		case v.Expect.ExpiresIn != nil:
			now := goldenParseTime(t, *v.State.Clock)
			if got := d.ExpiresAtUnixNano - now; got != *v.Expect.ExpiresIn*int64(time.Second) {
				t.Errorf("%s: expires %d ns after the clock, want %d s", v.ID, got, *v.Expect.ExpiresIn)
			}
		}
	}
	if ran != want {
		t.Fatalf("ran %d pdp rows, manifest holds %d", ran, want)
	}
	var missing []string
	for _, r := range AllReasons() {
		if reasons[r] == 0 {
			missing = append(missing, string(r))
		}
	}
	sort.Strings(missing)
	if len(missing) != 0 {
		t.Errorf("reasons with no pdp vector: %v", missing)
	}
}

func TestGoldenDecisionPolicyRevisionIsTheCatalogVersion(t *testing.T) {
	root := goldenRoot(t)
	dir := filepath.Join(root, "contracts/auth/v1/golden/decision")
	var v goldenVector
	goldenStrict(t, filepath.Join(dir, "vectors", "policy-revision-is-catalog-version.json"), &v)
	if v.Layer != "pdp" || v.Expect.PolicyRevision != policycatalog.Default().Version() {
		t.Fatalf("policy_revision %d, layer %q; catalog version %d", v.Expect.PolicyRevision, v.Layer, policycatalog.Default().Version())
	}
}
