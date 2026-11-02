package authapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/grant"
	"github.com/full-chaos/dev-health-ops/internal/auth/publicread"
)

type fakeReads struct {
	calls     int
	err       error
	org       publicread.Organization
	of        []publicread.OrganizationOf
	ofMore    bool
	mem       publicread.Membership
	mems      []publicread.Membership
	memsMore  bool
	prin      publicread.Principal
	prins     []publicread.Principal
	prinsMore bool

	after *uuid.UUID
	limit int
	kind  string
	ids   []uuid.UUID
}

func (f *fakeReads) note(after *uuid.UUID, limit int, ids ...uuid.UUID) {
	f.calls++
	f.after, f.limit, f.ids = after, limit, ids
}

func (f *fakeReads) Organization(_ context.Context, id uuid.UUID) (publicread.Organization, error) {
	f.note(nil, 0, id)
	return f.org, f.err
}

func (f *fakeReads) OrganizationsOf(_ context.Context, p uuid.UUID, after *uuid.UUID, limit int) ([]publicread.OrganizationOf, bool, error) {
	f.note(after, limit, p)
	return f.of, f.ofMore, f.err
}

func (f *fakeReads) Membership(_ context.Context, org, p uuid.UUID) (publicread.Membership, error) {
	f.note(nil, 0, org, p)
	return f.mem, f.err
}

func (f *fakeReads) Memberships(_ context.Context, org uuid.UUID, after *uuid.UUID, limit int) ([]publicread.Membership, bool, error) {
	f.note(after, limit, org)
	return f.mems, f.memsMore, f.err
}

func (f *fakeReads) Principal(_ context.Context, id uuid.UUID) (publicread.Principal, error) {
	f.note(nil, 0, id)
	return f.prin, f.err
}

func (f *fakeReads) Principals(_ context.Context, org uuid.UUID, kind string, after *uuid.UUID, limit int) ([]publicread.Principal, bool, error) {
	f.note(after, limit, org)
	f.kind = kind
	return f.prins, f.prinsMore, f.err
}

type fakeGrantReads struct {
	calls  int
	err    error
	one    grant.Grant
	all    []grant.Grant
	filter grant.ListFilter
	org    uuid.UUID
}

func (f *fakeGrantReads) Get(_ context.Context, org, _ uuid.UUID) (*grant.Grant, error) {
	f.calls++
	f.org = org
	if f.err != nil {
		return nil, f.err
	}
	g := f.one
	return &g, nil
}

func (f *fakeGrantReads) ListForOrg(_ context.Context, org uuid.UUID, filter grant.ListFilter) ([]grant.Grant, error) {
	f.calls++
	f.org, f.filter = org, filter
	return f.all, f.err
}

var readTime = time.Date(2026, 11, 2, 11, 30, 0, 123456000, time.UTC)

type readCase struct {
	op, path, scope, notFound string
	body                      func(f *provFixture) string
}

func wire(prefix string, id uuid.UUID) string { return prefix + id.String() }

func readCases() []readCase {
	org := func(f *provFixture) string { return wire("org_", f.org) }
	return []readCase{
		{"principals_get", PatternPrincipalsGet, "principals:read", reasonPrincipalNotFound,
			func(f *provFixture) string {
				return `{"schema_version":"principal-get-request.v1","principal_id":"` + wire("prn_", f.target) + `"}`
			}},
		{"principals_list", PatternPrincipalsList, "principals:read", "",
			func(f *provFixture) string {
				return `{"schema_version":"principal-list-request.v1","organization_id":"` + org(f) + `"}`
			}},
		{"organizations_get", PatternOrganizationsGet, "organizations:read", reasonOrganizationNotFound,
			func(f *provFixture) string {
				return `{"schema_version":"organization-get-request.v1","organization_id":"` + org(f) + `"}`
			}},
		{"organizations_list", PatternOrganizationsList, "organizations:read", "",
			func(f *provFixture) string {
				return `{"schema_version":"organization-list-request.v1","principal_id":"` + wire("prn_", f.target) + `"}`
			}},
		{"memberships_get", PatternMembershipsGet, "memberships:read", reasonMembershipNotFound,
			func(f *provFixture) string {
				return `{"schema_version":"membership-get-request.v1","organization_id":"` + org(f) + `","principal_id":"` + wire("prn_", f.target) + `"}`
			}},
		{"memberships_list", PatternMembershipsList, "memberships:read", "",
			func(f *provFixture) string {
				return `{"schema_version":"membership-list-request.v1","organization_id":"` + org(f) + `"}`
			}},
		{"grants_get", PatternGrantsGet, "grants:read", reasonGrantNotFound,
			func(f *provFixture) string {
				return `{"schema_version":"grant-get-request.v1","organization_id":"` + org(f) + `","grant_id":"` + wire("grt_", f.grants.out.ID) + `"}`
			}},
		{"grants_list", PatternGrantsList, "grants:read", "",
			func(f *provFixture) string {
				return `{"schema_version":"grant-list-request.v1","organization_id":"` + org(f) + `"}`
			}},
	}
}

func readPatterns() []string {
	var out []string
	for _, c := range readCases() {
		out = append(out, c.path)
	}
	return out
}

func newReadFixture(t *testing.T) *provFixture {
	t.Helper()
	f := newProvFixtureWith(t, docWithRows(t, append(readPatterns(), PatternOrgSuspend, PatternOrgRename)...))
	seedReads(f)
	return f
}

func seedReads(f *provFixture) {
	member := publicread.Membership{ID: uuid.New(), OrgID: f.org, PrincipalID: f.target, Role: "member", JoinedAt: &readTime,
		CreatedAt: readTime, UpdatedAt: readTime, Revision: 2}
	organization := publicread.Organization{ID: f.org, Slug: "acme", Name: "Acme", Active: true, CreatedAt: readTime, UpdatedAt: readTime, Revision: 4}
	f.reads.org, f.reads.mem, f.reads.mems = organization, member, []publicread.Membership{member}
	f.reads.of = []publicread.OrganizationOf{{Organization: organization, Membership: member}}
	f.reads.prin = publicread.Principal{ID: f.target, Kind: "user", Active: true, Revision: 3}
	f.reads.prins = []publicread.Principal{f.reads.prin}
	f.grantReads.one = f.grants.out
	f.grantReads.all = []grant.Grant{f.grants.out}
}

func TestReadRoutesNotMountedWithoutRow(t *testing.T) {
	f := newProvFixture(t)
	for _, c := range readCases() {
		response := f.post(c.path, c.body(f), nil)
		if response.Code != http.StatusNotFound {
			t.Errorf("%s without its row: %d, want 404", c.op, response.Code)
		}
	}
	if f.reads.calls+f.grantReads.calls != 0 || len(f.creds.scopes) != 0 {
		t.Fatal("an unmounted route reached a store or the credential check")
	}
	one := newProvFixtureWith(t, docWithRows(t, PatternPrincipalsGet, PatternOrgSuspend, PatternOrgRename))
	seedReads(one)
	for _, c := range readCases() {
		got := one.post(c.path, c.body(one), nil).Code
		if c.op == "principals_get" && got != http.StatusOK || c.op != "principals_get" && got != http.StatusNotFound {
			t.Errorf("only the principals_get row: %s answered %d", c.op, got)
		}
	}
}

func TestReadRoutesNotMountedWithoutStore(t *testing.T) {
	f := newReadFixture(t)
	f.mux = nil
	routes, err := ProvisioningRoutes(ProvisioningDeps{Credentials: f.creds, Users: f.users, Reader: noRows{}, Organizations: f.orgs,
		Memberships: f.members, Invites: f.invites, Deprovision: f.deprovision, Orgs: f.reactivate, ServiceAccounts: f.sa, Grants: f.grants,
		ProfileRows: docWithRows(t, append(readPatterns(), PatternOrgSuspend, PatternOrgRename)...)})
	if err != nil {
		t.Fatal(err)
	}
	for _, route := range routes {
		if slices.Contains(readPatterns(), route.Pattern) {
			t.Errorf("%s mounted with no read store", route.Pattern)
		}
	}
}

func TestReadScopeRequired(t *testing.T) {
	for _, c := range readCases() {
		f := newReadFixture(t)
		f.creds.held = []string{"memberships:write", "sessions:issue"}
		expectRefusal(t, f.post(c.path, c.body(f), nil), http.StatusUnauthorized, reasonInvalidCredentials)
		if f.reads.calls+f.grantReads.calls != 0 {
			t.Errorf("%s: a credential without the read scope reached a store", c.op)
		}
		if !reflect.DeepEqual(f.creds.scopes, []string{c.scope}) {
			t.Errorf("%s asked for scopes %v, want [%s]", c.op, f.creds.scopes, c.scope)
		}
		other := newReadFixture(t)
		other.creds.held = []string{"principals:read", "organizations:read", "memberships:read", "grants:read"}
		other.creds.held = slices.DeleteFunc(other.creds.held, func(s string) bool { return s == c.scope })
		expectRefusal(t, other.post(c.path, c.body(other), nil), http.StatusUnauthorized, reasonInvalidCredentials)
		held := newReadFixture(t)
		held.creds.held = []string{c.scope}
		if got := held.post(c.path, c.body(held), nil); got.Code != http.StatusOK {
			t.Errorf("%s with its scope: %d %s", c.op, got.Code, got.Body)
		}
	}
	f := newReadFixture(t)
	for _, header := range []map[string]string{{}, {"Authorization": "Bearer nonsense"}} {
		for _, c := range readCases() {
			expectRefusal(t, f.post(c.path, c.body(f), header), http.StatusUnauthorized, reasonInvalidCredentials)
		}
	}
	if f.reads.calls+f.grantReads.calls != 0 {
		t.Fatal("a refused bearer reached a store")
	}
}

func TestReadRoutesAnswerWithoutWriting(t *testing.T) {
	for _, c := range readCases() {
		f := newReadFixture(t)
		got := f.post(c.path, c.body(f), nil)
		if got.Code != http.StatusOK {
			t.Fatalf("%s: %d %s", c.op, got.Code, got.Body)
		}
		if f.storeCalls() != 0 || f.users.reads != 0 {
			t.Errorf("%s wrote or read an actor (store calls %d, user reads %d)", c.op, f.storeCalls(), f.users.reads)
		}
		if n, ok := f.uses(c.op); ok && n != 0 {
			t.Errorf("%s counted as a compat credential use", c.op)
		}
		if ct := got.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
			t.Errorf("%s content type %q", c.op, ct)
		}
	}
}

func TestReadRoutesNoAuditNoOutbox(t *testing.T) {
	src := readSources(t)
	for _, word := range []string{"audit.", "Outbox", "outbox", "Commit(", "Exec(", "Begin("} {
		if strings.Contains(src, word) {
			t.Errorf("the read routes' source names %q: reads write no audit row and no outbox row", word)
		}
	}
}

func readSources(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile("provisioning_read.go")
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func TestForeignAndMissingSameAnswer(t *testing.T) {
	for _, c := range readCases() {
		if c.notFound == "" {
			continue
		}
		var answers []string
		for _, label := range []string{"missing", "foreign"} {
			f := newReadFixture(t)
			f.reads.err, f.grantReads.err = publicread.ErrNotFound, grant.ErrNotFound
			got := f.post(c.path, c.body(f), nil)
			expectRefusal(t, got, http.StatusNotFound, c.notFound)
			var doc map[string]any
			_ = json.Unmarshal(got.Body.Bytes(), &doc)
			delete(doc, "request_id")
			delete(doc, "occurred_at")
			raw, _ := json.Marshal(doc)
			answers = append(answers, label+string(raw))
		}
		if strings.TrimPrefix(answers[0], "missing") != strings.TrimPrefix(answers[1], "foreign") {
			t.Errorf("%s: missing and foreign answers differ: %v", c.op, answers)
		}
	}
	f := newReadFixture(t)
	f.grantReads.err = grant.ErrNotFound
	f.post(PatternGrantsGet, readCases()[6].body(f), nil)
	if f.grantReads.org != f.org {
		t.Fatalf("grants/get read organization %s, want the request's %s: the lookup must be bound to it", f.grantReads.org, f.org)
	}
}

func TestReadStoreFailuresFailClosed(t *testing.T) {
	for _, c := range readCases() {
		for name, err := range map[string]error{"store": errors.New("db down"), "unknown role": publicread.ErrUnknownRole, "pkg store": publicread.ErrStore, "grant store": grant.ErrStore} {
			f := newReadFixture(t)
			f.reads.err, f.grantReads.err = err, err
			got := f.post(c.path, c.body(f), nil)
			if got.Code != http.StatusServiceUnavailable || reasonOf(t, got) != reasonStoreUnavailable {
				t.Errorf("%s with %s: %d %s", c.op, name, got.Code, got.Body)
			}
			if strings.Contains(got.Body.String(), "superhero") || strings.Contains(got.Body.String(), "db down") {
				t.Errorf("%s: the answer repeats store text: %s", c.op, got.Body)
			}
		}
	}
}

func TestReadRequestRules(t *testing.T) {
	f := newReadFixture(t)
	org := wire("org_", f.org)
	prn := wire("prn_", f.target)
	cursorOf := func(id string) string { return contracts.EncodeKeysetCursor(id) }
	cases := map[string]struct{ path, body string }{
		"no schema_version":      {PatternMembershipsList, `{"organization_id":"` + org + `"}`},
		"wrong schema_version":   {PatternMembershipsList, `{"schema_version":"membership-get-request.v1","organization_id":"` + org + `"}`},
		"extra member":           {PatternMembershipsList, `{"schema_version":"membership-list-request.v1","organization_id":"` + org + `","principal_id":"` + prn + `"}`},
		"duplicate member":       {PatternMembershipsList, `{"schema_version":"membership-list-request.v1","organization_id":"` + org + `","organization_id":"` + org + `"}`},
		"organization absent":    {PatternMembershipsList, `{"schema_version":"membership-list-request.v1"}`},
		"organization prefix":    {PatternMembershipsList, `{"schema_version":"membership-list-request.v1","organization_id":"` + prn + `"}`},
		"organization not uuid":  {PatternMembershipsList, `{"schema_version":"membership-list-request.v1","organization_id":"org_EXAMPLE"}`},
		"uppercase uuid":         {PatternMembershipsList, `{"schema_version":"membership-list-request.v1","organization_id":"org_` + strings.ToUpper(f.org.String()) + `"}`},
		"limit 0":                {PatternMembershipsList, `{"schema_version":"membership-list-request.v1","organization_id":"` + org + `","limit":0}`},
		"limit 101":              {PatternMembershipsList, `{"schema_version":"membership-list-request.v1","organization_id":"` + org + `","limit":101}`},
		"limit decimal":          {PatternMembershipsList, `{"schema_version":"membership-list-request.v1","organization_id":"` + org + `","limit":2.0}`},
		"limit string":           {PatternMembershipsList, `{"schema_version":"membership-list-request.v1","organization_id":"` + org + `","limit":"2"}`},
		"limit null":             {PatternMembershipsList, `{"schema_version":"membership-list-request.v1","organization_id":"` + org + `","limit":null}`},
		"cursor not k1":          {PatternMembershipsList, `{"schema_version":"membership-list-request.v1","organization_id":"` + org + `","cursor":"abcd"}`},
		"cursor wrong kind":      {PatternMembershipsList, `{"schema_version":"membership-list-request.v1","organization_id":"` + org + `","cursor":"` + cursorOf(prn) + `"}`},
		"cursor not uuid":        {PatternMembershipsList, `{"schema_version":"membership-list-request.v1","organization_id":"` + org + `","cursor":"` + cursorOf("mbr_EXAMPLE") + `"}`},
		"status unknown":         {PatternMembershipsList, `{"schema_version":"membership-list-request.v1","organization_id":"` + org + `","status":"pending"}`},
		"kind unknown":           {PatternPrincipalsList, `{"schema_version":"principal-list-request.v1","organization_id":"` + org + `","kind":"robot"}`},
		"principals need org":    {PatternPrincipalsList, `{"schema_version":"principal-list-request.v1"}`},
		"grant id wrong kind":    {PatternGrantsGet, `{"schema_version":"grant-get-request.v1","organization_id":"` + org + `","grant_id":"` + prn + `"}`},
		"include_revoked string": {PatternGrantsList, `{"schema_version":"grant-list-request.v1","organization_id":"` + org + `","include_revoked":"true"}`},
		"body is an array":       {PatternPrincipalsGet, `[]`},
		"body empty":             {PatternPrincipalsGet, ``},
		"membership status":      {PatternOrganizationsList, `{"schema_version":"organization-list-request.v1","principal_id":"` + prn + `","membership_status":"gone"}`},
	}
	names := make([]string, 0, len(cases))
	for n := range cases {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, name := range names {
		c := cases[name]
		expectRefusal(t, f.post(c.path, c.body, nil), http.StatusBadRequest, reasonMalformed)
	}
	if f.reads.calls+f.grantReads.calls != 0 {
		t.Fatal("a malformed request reached a store")
	}
}

func TestReadPagingPassesLimitAndCursor(t *testing.T) {
	f := newReadFixture(t)
	org := wire("org_", f.org)
	f.post(PatternMembershipsList, `{"schema_version":"membership-list-request.v1","organization_id":"`+org+`"}`, nil)
	if f.reads.limit != 50 || f.reads.after != nil {
		t.Fatalf("default: limit %d after %v, want 50 and none", f.reads.limit, f.reads.after)
	}
	after := uuid.New()
	f.post(PatternMembershipsList, `{"schema_version":"membership-list-request.v1","organization_id":"`+org+`","limit":100,"cursor":"`+contracts.EncodeKeysetCursor(wire("mbr_", after))+`"}`, nil)
	if f.reads.limit != 100 || f.reads.after == nil || *f.reads.after != after {
		t.Fatalf("limit %d after %v, want 100 and %s", f.reads.limit, f.reads.after, after)
	}
	f.post(PatternOrganizationsList, `{"schema_version":"organization-list-request.v1","principal_id":"`+wire("prn_", f.target)+`","limit":7,"cursor":"`+contracts.EncodeKeysetCursor(wire("org_", after))+`"}`, nil)
	if f.reads.limit != 7 || f.reads.after == nil || *f.reads.after != after {
		t.Fatalf("organizations: limit %d after %v", f.reads.limit, f.reads.after)
	}
	f.post(PatternPrincipalsList, `{"schema_version":"principal-list-request.v1","organization_id":"`+org+`","kind":"workload"}`, nil)
	if f.reads.kind != "workload" || f.reads.ids[0] != f.org {
		t.Fatalf("principals list: kind %q org %v", f.reads.kind, f.reads.ids)
	}
}

func TestReadNextCursorIsTheLastItemAndAbsentOnTheLastPage(t *testing.T) {
	f := newReadFixture(t)
	ids := []uuid.UUID{uuid.New(), uuid.New()}
	sort.Slice(ids, func(i, j int) bool { return ids[i].String() < ids[j].String() })
	f.reads.mems = nil
	for _, id := range ids {
		f.reads.mems = append(f.reads.mems, publicread.Membership{ID: id, OrgID: f.org, PrincipalID: uuid.New(), Role: "admin", CreatedAt: readTime, UpdatedAt: readTime})
	}
	body := `{"schema_version":"membership-list-request.v1","organization_id":"` + wire("org_", f.org) + `","limit":2}`
	f.reads.memsMore = true
	var page struct {
		Items      []map[string]any `json:"items"`
		NextCursor *string          `json:"next_cursor"`
	}
	got := f.post(PatternMembershipsList, body, nil)
	if err := json.Unmarshal(got.Body.Bytes(), &page); err != nil || len(page.Items) != 2 || page.NextCursor == nil {
		t.Fatalf("first page: %s", got.Body)
	}
	if want := contracts.EncodeKeysetCursor(wire("mbr_", ids[1])); *page.NextCursor != want {
		t.Fatalf("next_cursor = %s, want the last item's %s", *page.NextCursor, want)
	}
	f.reads.memsMore = false
	got = f.post(PatternMembershipsList, body, nil)
	if strings.Contains(got.Body.String(), "next_cursor") {
		t.Fatalf("the last page carries next_cursor: %s", got.Body)
	}
	f.reads.mems = nil
	got = f.post(PatternMembershipsList, body, nil)
	if strings.Contains(got.Body.String(), "next_cursor") || !strings.Contains(got.Body.String(), `"items":[]`) {
		t.Fatalf("an empty page: %s", got.Body)
	}
}

func TestMembershipStatusFilterServesActiveOnly(t *testing.T) {
	f := newReadFixture(t)
	org := wire("org_", f.org)
	for _, status := range []string{"invited", "suspended", "removed"} {
		got := f.post(PatternMembershipsList, `{"schema_version":"membership-list-request.v1","organization_id":"`+org+`","status":"`+status+`"}`, nil)
		if got.Code != http.StatusOK || !strings.Contains(got.Body.String(), `"items":[]`) {
			t.Errorf("status %s: %d %s", status, got.Code, got.Body)
		}
		got = f.post(PatternOrganizationsList, `{"schema_version":"organization-list-request.v1","principal_id":"`+wire("prn_", f.target)+`","membership_status":"`+status+`"}`, nil)
		if got.Code != http.StatusOK || !strings.Contains(got.Body.String(), `"items":[]`) {
			t.Errorf("membership_status %s: %d %s", status, got.Code, got.Body)
		}
	}
	got := f.post(PatternMembershipsList, `{"schema_version":"membership-list-request.v1","organization_id":"`+org+`","status":"active"}`, nil)
	if !strings.Contains(got.Body.String(), `"status":"active"`) {
		t.Errorf("status active: %s", got.Body)
	}
}

func TestOrganizationStatusFollowsIsActive(t *testing.T) {
	f := newReadFixture(t)
	f.reads.org.Active = false
	got := f.post(PatternOrganizationsGet, readCases()[2].body(f), nil)
	if !strings.Contains(got.Body.String(), `"status":"suspended"`) {
		t.Fatalf("%s", got.Body)
	}
	f.reads.prin.Active = false
	got = f.post(PatternPrincipalsGet, readCases()[0].body(f), nil)
	if !strings.Contains(got.Body.String(), `"status":"disabled"`) {
		t.Fatalf("%s", got.Body)
	}
}

func TestPrincipalAnswerHasOnlyItsFiveMembers(t *testing.T) {
	f := newReadFixture(t)
	for _, c := range readCases()[:2] {
		var doc map[string]any
		got := f.post(c.path, c.body(f), nil)
		if err := json.Unmarshal(got.Body.Bytes(), &doc); err != nil {
			t.Fatal(err)
		}
		if items, ok := doc["items"].([]any); ok {
			doc = items[0].(map[string]any)
		}
		var keys []string
		for k := range doc {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		if want := []string{"kind", "principal_id", "revision", "schema_version", "status"}; !reflect.DeepEqual(keys, want) {
			t.Errorf("%s members = %v, want %v", c.op, keys, want)
		}
	}
}

func TestGrantListPagesAndFilters(t *testing.T) {
	f := newReadFixture(t)
	var all []grant.Grant
	for i := 0; i < 3; i++ {
		g := f.grants.out
		g.ID = uuid.MustParse(fmt.Sprintf("00000000-0000-4000-8000-%012d", i+1))
		all = append(all, g)
	}
	f.grantReads.all = all
	org := wire("org_", f.org)
	first := f.post(PatternGrantsList, `{"schema_version":"grant-list-request.v1","organization_id":"`+org+`","limit":2,"principal_id":"`+wire("prn_", f.target)+`","include_revoked":true}`, nil)
	var page struct {
		Items      []map[string]any `json:"items"`
		NextCursor *string          `json:"next_cursor"`
	}
	if err := json.Unmarshal(first.Body.Bytes(), &page); err != nil || len(page.Items) != 2 || page.NextCursor == nil {
		t.Fatalf("first page %s", first.Body)
	}
	if *page.NextCursor != contracts.EncodeKeysetCursor(wire("grt_", all[1].ID)) {
		t.Fatalf("next_cursor %s", *page.NextCursor)
	}
	if f.grantReads.filter.PrincipalID == nil || *f.grantReads.filter.PrincipalID != f.target || !f.grantReads.filter.IncludeRevoked {
		t.Fatalf("filter %+v", f.grantReads.filter)
	}
	second := f.post(PatternGrantsList, `{"schema_version":"grant-list-request.v1","organization_id":"`+org+`","limit":2,"cursor":"`+*page.NextCursor+`"}`, nil)
	page.Items, page.NextCursor = nil, nil
	if err := json.Unmarshal(second.Body.Bytes(), &page); err != nil || len(page.Items) != 1 || page.NextCursor != nil {
		t.Fatalf("second page %s", second.Body)
	}
	if page.Items[0]["grant_id"] != wire("grt_", all[2].ID) {
		t.Fatalf("second page starts at %v", page.Items[0]["grant_id"])
	}
	if f.grantReads.filter.IncludeRevoked || f.grantReads.filter.PrincipalID != nil {
		t.Fatalf("a request without the filters passed %+v", f.grantReads.filter)
	}
}

func TestGrantStatusRevokedThenExpiredThenActive(t *testing.T) {
	f := newReadFixture(t)
	past, future := time.Now().Add(-time.Hour), time.Now().Add(time.Hour)
	for _, c := range []struct {
		revoked, expires *time.Time
		want             string
	}{{nil, nil, "active"}, {nil, &future, "active"}, {nil, &past, "expired"}, {&past, &past, "revoked"}, {&past, nil, "revoked"}} {
		g := f.grants.out
		g.RevokedAt, g.ExpiresAt = c.revoked, c.expires
		f.grantReads.one = g
		got := f.post(PatternGrantsGet, readCases()[6].body(f), nil)
		if !strings.Contains(got.Body.String(), `"status":"`+c.want+`"`) {
			t.Errorf("want %s: %s", c.want, got.Body)
		}
	}
	f.grantReads.one = f.grants.out
	got := f.post(PatternGrantsGet, readCases()[6].body(f), nil)
	if !strings.Contains(got.Body.String(), `"resource_key":"acme/api"`) || !strings.Contains(got.Body.String(), `"expires_at":null`) {
		t.Errorf("grant answer %s", got.Body)
	}
}

// The handler's answers are the documents of the PR-1 corpus: for each exchange the parser accepts, the store is loaded with the
// corpus response and the answer must be the corpus response (ids and cursors normalized; the corpus uses example ids, the store uuids).
func TestReadAnswersMatchThePR1Corpus(t *testing.T) {
	root := repoRoot(t)
	dir := filepath.Join(root, "contracts", "auth", "v1", "golden", "read-api", "vectors")
	files, err := filepath.Glob(filepath.Join(dir, "*.json"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no corpus at %s", dir)
	}
	checked := 0
	for _, file := range files {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		var v struct {
			ID        string          `json:"id"`
			Kind      string          `json:"kind"`
			Operation string          `json:"operation"`
			Request   json.RawMessage `json:"request"`
			Response  json.RawMessage `json:"response"`
			Parse     string          `json:"parse"`
		}
		if err := json.Unmarshal(raw, &v); err != nil {
			t.Fatal(err)
		}
		if v.Kind != "exchange" || v.Parse != "accept" {
			continue
		}
		checked++
		t.Run(v.ID, func(t *testing.T) {
			f := newReadFixture(t)
			request := mapIDs(string(v.Request))
			loadCorpus(t, f, v.Operation, v.Response)
			var path string
			for _, c := range readCases() {
				if c.op == v.Operation {
					path = c.path
				}
			}
			got := f.post(path, request, nil)
			if got.Code != http.StatusOK {
				t.Fatalf("%d %s", got.Code, got.Body)
			}
			req, err := contracts.ParseReadRequest(root, v.Operation, []byte(request))
			if err != nil {
				t.Fatalf("request: %v", err)
			}
			if _, err := contracts.ParseReadResponse(root, req, got.Body.Bytes()); err != nil {
				t.Fatalf("the answer does not parse as the contract's response: %v\n%s", err, got.Body)
			}
			var have, want any
			if err := json.Unmarshal(got.Body.Bytes(), &have); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal([]byte(normalizeIDs(string(v.Response))), &want); err != nil {
				t.Fatal(err)
			}
			haveRaw, _ := json.Marshal(normalizeAny(have))
			wantRaw, _ := json.Marshal(want)
			if string(haveRaw) != string(wantRaw) {
				t.Fatalf("answer differs from the corpus\n got %s\nwant %s", haveRaw, wantRaw)
			}
		})
	}
	if checked < 12 {
		t.Fatalf("only %d corpus exchanges checked", checked)
	}
}

var idToken = regexp.MustCompile(`"((?:org|mbr|prn|grt)_)([A-Za-z0-9_-]+)"`)

// uuidFor maps an example id to a uuid that keeps the order of the example ids (they end in a counter).
func uuidFor(s string) uuid.UUID {
	n := 0
	for _, c := range s[strings.LastIndexFunc(s, func(r rune) bool { return r < '0' || r > '9' })+1:] {
		n = n*10 + int(c-'0')
	}
	return uuid.MustParse(fmt.Sprintf("00000000-0000-4000-8000-%012d", n))
}

// mapIDs gives every example id in text the uuid form of the same prefix.
func mapIDs(text string) string {
	text = idToken.ReplaceAllStringFunc(text, func(m string) string {
		p := idToken.FindStringSubmatch(m)
		return `"` + p[1] + uuidFor(p[1]+p[2]).String() + `"`
	})
	return mapCursors(text)
}

var cursorToken = regexp.MustCompile(`"(k1_[A-Za-z0-9_-]+)"`)

func mapCursors(text string) string {
	return cursorToken.ReplaceAllStringFunc(text, func(m string) string {
		c := cursorToken.FindStringSubmatch(m)[1]
		for _, prefix := range []string{"org_", "mbr_", "prn_", "grt_"} {
			id, err := contracts.DecodeKeysetCursor(c, prefix)
			if err == nil {
				return `"` + contracts.EncodeKeysetCursor(prefix+uuidFor(id).String()) + `"`
			}
		}
		return m
	})
}

// normalizeIDs replaces every id and cursor by its prefix, so two documents that differ only in ids compare equal.
func normalizeIDs(text string) string {
	text = idToken.ReplaceAllString(text, `"${1}X"`)
	return cursorToken.ReplaceAllString(text, `"k1_X"`)
}

func normalizeAny(v any) any {
	raw, _ := json.Marshal(v)
	var out any
	_ = json.Unmarshal([]byte(normalizeIDs(string(raw))), &out)
	return out
}

func parseTime(t *testing.T, s any) time.Time {
	t.Helper()
	text, _ := s.(string)
	v, err := time.Parse(time.RFC3339Nano, text)
	if err != nil {
		t.Fatalf("time %v: %v", s, err)
	}
	return v
}

func uid(t *testing.T, wireID any) uuid.UUID {
	t.Helper()
	text, _ := wireID.(string)
	return uuidFor(text)
}

func loadCorpus(t *testing.T, f *provFixture, op string, response json.RawMessage) {
	t.Helper()
	var doc map[string]any
	if err := json.Unmarshal(response, &doc); err != nil {
		t.Fatal(err)
	}
	_, hasCursor := doc["next_cursor"]
	membership := func(m map[string]any) publicread.Membership {
		out := publicread.Membership{ID: uid(t, m["membership_id"]), OrgID: uid(t, m["organization_id"]), PrincipalID: uid(t, m["principal_id"]),
			Role: fmt.Sprint(m["role"]), CreatedAt: parseTime(t, m["created_at"]), UpdatedAt: parseTime(t, m["updated_at"]), Revision: int64(m["revision"].(float64))}
		if m["joined_at"] != nil {
			j := parseTime(t, m["joined_at"])
			out.JoinedAt = &j
		}
		return out
	}
	organization := func(o map[string]any) publicread.Organization {
		return publicread.Organization{ID: uid(t, o["organization_id"]), Slug: o["slug"].(string), Name: o["name"].(string), Active: o["status"] == "active",
			CreatedAt: parseTime(t, o["created_at"]), UpdatedAt: parseTime(t, o["updated_at"]), Revision: int64(o["revision"].(float64))}
	}
	principal := func(p map[string]any) publicread.Principal {
		return publicread.Principal{ID: uid(t, p["principal_id"]), Kind: p["kind"].(string), Active: p["status"] == "active", Revision: int64(p["revision"].(float64))}
	}
	gr := func(g map[string]any) grant.Grant {
		out := grant.Grant{ID: uid(t, g["grant_id"]), OrgID: uid(t, g["organization_id"]), PrincipalID: uid(t, g["principal_id"]), Role: g["role"].(string),
			Key: g["resource_key"].(string), CreatedAt: parseTime(t, g["created_at"]), Revision: int64(g["revision"].(float64))}
		if g["expires_at"] != nil {
			e := parseTime(t, g["expires_at"])
			out.ExpiresAt = &e
		}
		switch g["status"] {
		case "revoked":
			r := readTime
			out.RevokedAt = &r
		case "expired":
			e := time.Now().Add(-time.Hour)
			if out.ExpiresAt == nil {
				out.ExpiresAt = &e
			}
		}
		return out
	}
	items, _ := doc["items"].([]any)
	switch op {
	case "organizations_get":
		f.reads.org = organization(doc)
	case "memberships_get":
		f.reads.mem = membership(doc)
	case "principals_get":
		f.reads.prin = principal(doc)
	case "grants_get":
		f.grantReads.one = gr(doc)
	case "memberships_list":
		f.reads.mems, f.reads.memsMore = nil, hasCursor
		for _, it := range items {
			f.reads.mems = append(f.reads.mems, membership(it.(map[string]any)))
		}
	case "principals_list":
		f.reads.prins, f.reads.prinsMore = nil, hasCursor
		for _, it := range items {
			f.reads.prins = append(f.reads.prins, principal(it.(map[string]any)))
		}
	case "organizations_list":
		f.reads.of, f.reads.ofMore = nil, hasCursor
		for _, it := range items {
			m := it.(map[string]any)
			f.reads.of = append(f.reads.of, publicread.OrganizationOf{Organization: organization(m["organization"].(map[string]any)), Membership: membership(m["membership"].(map[string]any))})
		}
	case "grants_list":
		f.grantReads.all = nil
		var last grant.Grant
		for _, it := range items {
			last = gr(it.(map[string]any))
			f.grantReads.all = append(f.grantReads.all, last)
		}
		if hasCursor {
			extra := last
			extra.ID = uuid.MustParse("ffffffff-ffff-4fff-8fff-ffffffffffff")
			f.grantReads.all = append(f.grantReads.all, extra)
		}
	default:
		t.Fatalf("no loader for %s", op)
	}
}

// public.memberships.joined_at is nullable and the contract needs one for an active member: a row without it reads as joined when
// it was created.
func TestMembershipWithoutJoinedAtReadsAsJoinedWhenCreated(t *testing.T) {
	f := newReadFixture(t)
	f.reads.mem.JoinedAt = nil
	got := f.post(PatternMembershipsGet, readCases()[4].body(f), nil)
	if !strings.Contains(got.Body.String(), `"joined_at":"`+stamp(readTime)+`"`) {
		t.Fatalf("%s", got.Body)
	}
}
