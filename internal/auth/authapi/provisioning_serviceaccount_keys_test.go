package authapi

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/serviceaccount"
)

// fakeKeys answers the key verbs of fakeServiceAccounts: each runs its Authorize as the store does after its locks.
type fakeKeys struct {
	key        serviceaccount.Key
	secret     string
	keyErr     error // KeyByID, first read
	afterErr   error // KeyByID, the answer read after a revoke
	listErr    error
	keyReads   int
	expiresIn  time.Duration
	overlap    time.Duration
	revoked    string
	revokedKey uuid.UUID
}

func (f *fakeServiceAccounts) locks() serviceaccount.Locked {
	return serviceaccount.Locked{OrgID: f.info.OrgID, Owner: f.info.Owner}
}

func (f *fakeServiceAccounts) issued() serviceaccount.IssuedKey {
	return serviceaccount.IssuedKey{Secret: contracts.NewSecretString(f.secret), Key: f.key}
}

func (f *fakeServiceAccounts) IssueKey(_ context.Context, _ uuid.UUID, expiresIn time.Duration, a serviceaccount.Actor) (serviceaccount.IssuedKey, error) {
	f.expiresIn = expiresIn
	if err := f.run("issue", a, f.locks()); err != nil {
		return serviceaccount.IssuedKey{}, err
	}
	return f.issued(), nil
}

func (f *fakeServiceAccounts) RotateKey(_ context.Context, _ uuid.UUID, overlap, expiresIn time.Duration, a serviceaccount.Actor) (serviceaccount.IssuedKey, error) {
	f.overlap, f.expiresIn = overlap, expiresIn
	if err := f.run("rotate", a, f.locks()); err != nil {
		return serviceaccount.IssuedKey{}, err
	}
	parent := uuid.New()
	out := f.issued()
	out.Key.ParentID = &parent
	return out, nil
}

func (f *fakeServiceAccounts) RevokeKey(_ context.Context, id uuid.UUID, reason string, a serviceaccount.Actor) error {
	f.revokedKey, f.revoked = id, reason
	if err := f.run("revoke", a, f.locks()); err != nil {
		return err
	}
	at := f.key.CreatedAt.Add(time.Minute)
	f.key.RevokedAt, f.key.RevokedReason = &at, &reason
	return nil
}

func (f *fakeServiceAccounts) ListKeys(context.Context, uuid.UUID) ([]serviceaccount.Key, error) {
	f.calls++
	if f.listErr != nil {
		return nil, f.listErr
	}
	return []serviceaccount.Key{f.key}, nil
}

func (f *fakeServiceAccounts) KeyByID(_ context.Context, id uuid.UUID) (*serviceaccount.Key, error) {
	f.keyReads++
	if f.keyReads > 1 && f.afterErr != nil {
		return nil, f.afterErr
	}
	if f.keyErr != nil {
		return nil, f.keyErr
	}
	if id != f.key.ID {
		return nil, serviceaccount.ErrKeyNotFound
	}
	k := f.key
	return &k, nil
}

// docWithRows is the embedded document with the rows for routes replaced by copies of the deprovision row.
func docWithRows(t testing.TB, routes ...string) []byte {
	t.Helper()
	var document map[string]any
	if err := json.Unmarshal(authv1.AuthServiceEndpointProfiles, &document); err != nil {
		t.Fatal(err)
	}
	var kept []any
	var base map[string]any
	for _, item := range document["rows"].([]any) {
		row := item.(map[string]any)
		if row["route"] == PatternOrgDeprovision && row["service"] == AccessService {
			base = row
		}
		if !slices.Contains(routes, row["route"].(string)) {
			kept = append(kept, item)
		}
	}
	if base == nil {
		t.Fatal("no deprovision row in the embedded document")
	}
	seen := map[string]bool{}
	for _, route := range routes {
		if seen[route] {
			continue
		}
		seen[route] = true
		clone := map[string]any{}
		for k, v := range base {
			clone[k] = v
		}
		clone["route"] = route
		clone["id"] = "POST " + route + " [dev-health-auth-service @api]"
		kept = append(kept, clone)
	}
	document["rows"] = kept
	raw, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func newKeyFixture(t *testing.T) *provFixture {
	t.Helper()
	return newProvFixture(t)
}

func seedKey(f *provFixture) {
	value, err := serviceaccount.MintKey()
	if err != nil {
		f.t.Fatal(err)
	}
	digest := serviceaccount.KeyDigest(value)
	created := time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC)
	f.sa.secret = value
	f.sa.key = serviceaccount.Key{ID: uuid.New(), Principal: f.sa.info.ID, CreatedAt: created, UpdatedAt: created, ExpiresAt: time.Now().Add(24 * time.Hour),
		Fingerprint: hex.EncodeToString(digest[:8]), CreatedBy: &f.admin}
}

func keyCases() []routeCase {
	return []routeCase{
		{opServiceAccountKeyIssue, PatternServiceAccountKeyIssue, func(f *provFixture, a uuid.UUID) map[string]any {
			return map[string]any{"service_account_id": f.sa.info.ID.String(), "actor_principal_id": a.String()}
		}, []string{"service_account_id", "actor_principal_id"}},
		{opServiceAccountKeyRotate, PatternServiceAccountKeyRotate, func(f *provFixture, a uuid.UUID) map[string]any {
			return map[string]any{"service_account_id": f.sa.info.ID.String(), "actor_principal_id": a.String(), "overlap_seconds": 3600}
		}, []string{"service_account_id", "actor_principal_id"}},
		{opServiceAccountKeyRevoke, PatternServiceAccountKeyRevoke, func(f *provFixture, a uuid.UUID) map[string]any {
			return map[string]any{"credential_id": "crd_" + f.sa.key.ID.String(), "actor_principal_id": a.String(), "reason": "compromised"}
		}, []string{"credential_id", "actor_principal_id", "reason"}},
		{opServiceAccountKeyList, PatternServiceAccountKeyList, func(f *provFixture, a uuid.UUID) map[string]any {
			return map[string]any{"service_account_id": f.sa.info.ID.String(), "actor_principal_id": a.String()}
		}, []string{"service_account_id", "actor_principal_id"}},
	}
}

func TestKeyRouteAnswersParseByTheirSchema(t *testing.T) {
	root := repoRoot(t)
	f := newKeyFixture(t)
	cases := keyCases()
	response := f.post(cases[0].path, encode(cases[0].body(f, f.admin)), nil)
	if response.Code != http.StatusOK || response.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("issue: %d, Cache-Control %q: %s", response.Code, response.Header().Get("Cache-Control"), response.Body)
	}
	issued, err := contracts.ParseCredentialReceipt(root, response.Body.Bytes(), time.Time{})
	if err != nil {
		t.Fatalf("issue answer: %v", err)
	}
	if issued.Kind != "issue" || issued.Secret != f.sa.secret || issued.Credential.CredentialClass != "service_account_key" || issued.Credential.DisplayPrefix != "fcsak_" ||
		issued.Credential.Status != "active" || issued.Credential.DomainOwner != "platform" || len(issued.Credential.Audiences) != 0 || len(issued.Credential.Scopes) != 0 {
		t.Fatalf("issue receipt: %+v", issued.Credential)
	}
	response = f.post(cases[1].path, encode(cases[1].body(f, f.admin)), nil)
	rotated, err := contracts.ParseCredentialReceipt(root, response.Body.Bytes(), time.Time{})
	if response.Code != http.StatusOK || err != nil || rotated.Kind != "rotate" || rotated.Credential.RotationParentID == "" {
		t.Fatalf("rotate: %d %v %s", response.Code, err, response.Body)
	}
	response = f.post(cases[3].path, encode(cases[3].body(f, f.admin)), nil)
	list, err := contracts.ParseCredentialList(root, response.Body.Bytes(), time.Time{})
	if response.Code != http.StatusOK || err != nil || len(list.Items) != 1 || list.NextCursor != nil {
		t.Fatalf("list: %d %v %s", response.Code, err, response.Body)
	}
	response = f.post(cases[2].path, encode(cases[2].body(f, f.admin)), nil)
	entry, err := contracts.ParseCredentialEntry(root, response.Body.Bytes(), time.Time{})
	if response.Code != http.StatusOK || err != nil || entry.Status != "revoked" || entry.RevokedReason != "compromised" || entry.RevokedAt == nil {
		t.Fatalf("revoke: %d %v %s", response.Code, err, response.Body)
	}
	if f.sa.revoked != "compromised" || f.sa.revokedKey != f.sa.key.ID {
		t.Fatalf("the verb got %q on %s", f.sa.revoked, f.sa.revokedKey)
	}
	// A repeat answers the entry; the store writes nothing.
	response = f.post(cases[2].path, encode(cases[2].body(f, f.admin)), nil)
	if _, err := contracts.ParseCredentialEntry(root, response.Body.Bytes(), time.Time{}); response.Code != http.StatusOK || err != nil {
		t.Fatalf("repeat revoke: %d %v", response.Code, err)
	}
}

// The value leaves in the issue and rotate answers only. The fingerprint is 8 digest bytes by design: it is exactly 16
// hex, the row's own, and the whole digest is in no answer and no log line.
func TestKeyRouteSecretAndDigestReachOnlyWhereTheyShould(t *testing.T) {
	f := newKeyFixture(t)
	digest := serviceaccount.KeyDigest(f.sa.secret)
	whole := []string{hex.EncodeToString(digest[:]), base64.StdEncoding.EncodeToString(digest[:]), base64.RawURLEncoding.EncodeToString(digest[:])}
	fingerprint := regexp.MustCompile(`"fingerprint":"([^"]*)"`)
	for i, c := range keyCases() {
		response := f.post(c.path, encode(c.body(f, f.admin)), nil)
		if response.Code != http.StatusOK {
			t.Fatalf("%s: %d %s", c.op, response.Code, response.Body)
		}
		text := response.Body.String()
		holds := strings.Contains(text, f.sa.secret)
		if want := i < 2; holds != want {
			t.Fatalf("%s: the answer holds the value = %v, want %v", c.op, holds, want)
		}
		match := fingerprint.FindStringSubmatch(text)
		if match == nil || !regexp.MustCompile(`^[0-9a-f]{16}$`).MatchString(match[1]) || match[1] != f.sa.key.Fingerprint {
			t.Fatalf("%s: fingerprint %v, want the row's 16 hex %s", c.op, match, f.sa.key.Fingerprint)
		}
		for _, form := range whole {
			if strings.Contains(text, form) {
				t.Fatalf("%s: the answer holds the whole digest", c.op)
			}
		}
		f.sa.key.RevokedAt, f.sa.key.RevokedReason = nil, nil
	}
	logs := f.logs.String()
	if strings.Contains(logs, f.sa.secret[len("fcsak_"):]) {
		t.Fatal("a log line holds the value")
	}
	for _, form := range whole {
		if strings.Contains(logs, form) {
			t.Fatal("a log line holds the whole digest")
		}
	}
}

func TestKeyRouteExpiryDefaultAndBounds(t *testing.T) {
	f := newKeyFixture(t)
	for _, c := range keyCases()[:2] {
		if response := f.post(c.path, encode(c.body(f, f.admin)), nil); response.Code != http.StatusOK {
			t.Fatalf("%s: %d", c.op, response.Code)
		}
		if f.sa.expiresIn != 7776000*time.Second {
			t.Fatalf("%s default expiry %s, want 7776000 s", c.op, f.sa.expiresIn)
		}
		for seconds, ok := range map[int64]bool{0: false, 1: true, 31536000: true, 31536001: false} {
			response := f.post(c.path, encode(merged(c.body(f, f.admin), map[string]any{"expires_in_seconds": seconds})), nil)
			if ok && (response.Code != http.StatusOK || f.sa.expiresIn != time.Duration(seconds)*time.Second) {
				t.Fatalf("%s expires %d: %d, verb got %s", c.op, seconds, response.Code, f.sa.expiresIn)
			}
			if !ok {
				expectRefusal(t, response, http.StatusBadRequest, reasonMalformed)
			}
		}
	}
}

func TestKeyRouteNullExpiryIsMalformed(t *testing.T) {
	f := newKeyFixture(t)
	for _, c := range keyCases()[:2] {
		response := f.post(c.path, encode(merged(c.body(f, f.admin), map[string]any{"expires_in_seconds": nil})), nil)
		if response.Code == http.StatusOK {
			t.Errorf("%s: a null expiry got the default: verb got %s", c.op, f.sa.expiresIn)
			continue
		}
		expectRefusal(t, response, http.StatusBadRequest, reasonMalformed)
	}
}

func TestKeyRouteOverlapIsRequiredAndBounded(t *testing.T) {
	f := newKeyFixture(t)
	c := keyCases()[1]
	expectRefusal(t, f.post(c.path, encode(without(c.body(f, f.admin), "overlap_seconds")), nil), http.StatusBadRequest, reasonMalformed)
	for seconds, ok := range map[int64]bool{-1: false, 0: true, 86400: true, 86401: false} {
		response := f.post(c.path, encode(merged(c.body(f, f.admin), map[string]any{"overlap_seconds": seconds})), nil)
		if ok && (response.Code != http.StatusOK || f.sa.overlap != time.Duration(seconds)*time.Second) {
			t.Fatalf("overlap %d: %d, verb got %s", seconds, response.Code, f.sa.overlap)
		}
		if !ok {
			expectRefusal(t, response, http.StatusBadRequest, reasonMalformed)
		}
	}
}

func TestKeyRouteBodiesAreStrict(t *testing.T) {
	f := newKeyFixture(t)
	for _, c := range keyCases() {
		valid := c.body(f, f.super)
		bad := map[string]string{
			"not an object":  `[]`,
			"trailing data":  encode(valid) + `{}`,
			"unknown member": encode(merged(valid, map[string]any{"schema_version": "x"})),
			"nested member":  encode(merged(valid, map[string]any{c.required[0]: map[string]any{"a": 1}})),
			"duplicate":      strings.Replace(encode(valid), "{", `{"`+c.required[0]+`":null,`, 1),
		}
		for _, name := range c.required {
			bad["missing "+name] = encode(without(valid, name))
			bad["null "+name] = encode(merged(valid, map[string]any{name: nil}))
		}
		for name, body := range bad {
			response := f.post(c.path, body, nil)
			if response.Code != http.StatusBadRequest || reasonOf(t, response) != reasonMalformed {
				t.Fatalf("%s %s: %d %s, want 400 malformed", c.op, name, response.Code, response.Body)
			}
		}
	}
	revoke := keyCases()[2]
	for name, id := range map[string]string{"bare uuid": f.sa.key.ID.String(), "upper case": "crd_" + strings.ToUpper(f.sa.key.ID.String()), "no uuid": "crd_x"} {
		response := f.post(revoke.path, encode(merged(revoke.body(f, f.admin), map[string]any{"credential_id": id})), nil)
		if response.Code != http.StatusBadRequest || reasonOf(t, response) != reasonMalformed {
			t.Fatalf("revoke %s: %d %s", name, response.Code, response.Body)
		}
	}
	if f.sa.calls != 0 || f.sa.keyReads != 0 {
		t.Fatal("a malformed body reached the store")
	}
}

func TestKeyRouteRefusalAnswers(t *testing.T) {
	f := newKeyFixture(t)
	cases := keyCases()[:3]
	for err, want := range map[error]struct {
		status int
		reason string
	}{
		serviceaccount.ErrAccountDisabled: {409, "service_account_disabled"},
		serviceaccount.ErrKeyLive:         {409, "key_live"},
		serviceaccount.ErrKeyRotating:     {409, "key_rotating"},
		serviceaccount.ErrKeyNone:         {409, "key_none"},
		serviceaccount.ErrKeyNotFound:     {404, "key_not_found"},
		serviceaccount.ErrKeyExpiry:       {400, "expiry_invalid"},
		serviceaccount.ErrKeyOverlap:      {400, "overlap_invalid"},
		serviceaccount.ErrKeyReason:       {400, "reason_invalid"},
		serviceaccount.ErrOrgNotActive:    {409, reasonOrganizationInactive},
		serviceaccount.ErrOwnerDisabled:   {409, "owner_invalid"},
		serviceaccount.ErrOwnerNotMember:  {409, "owner_not_member"},
		serviceaccount.ErrNotOwned:        {409, reasonUsersNotOwned},
		serviceaccount.ErrStateChanged:    {409, reasonStateChanged},
		serviceaccount.ErrStore:           {503, reasonStoreUnavailable},
	} {
		f.sa.err = err
		for _, c := range cases {
			expectRefusal(t, f.post(c.path, encode(c.body(f, f.admin)), nil), want.status, want.reason)
		}
	}
	f.sa.err = nil
	f.sa.listErr = errors.New("read failed")
	c := keyCases()[3]
	expectRefusal(t, f.post(c.path, encode(c.body(f, f.admin)), nil), http.StatusServiceUnavailable, reasonStoreUnavailable)
}

func TestKeyRouteActorRule(t *testing.T) {
	f := newKeyFixture(t)
	f.users.states[f.x] = UserState{IsActive: false, IsSuperuser: true}
	for _, c := range keyCases() {
		for name, tc := range map[string]struct {
			actor  uuid.UUID
			reason string
		}{
			"unknown actor":      {uuid.New(), reasonDeactivated},
			"inactive superuser": {f.x, reasonDeactivated},
			"viewer of the org":  {f.viewer, reasonAdminRequired},
			"member of no org":   {f.target, reasonAdminRequired},
		} {
			before := f.sa.calls
			expectRefusal(t, f.post(c.path, encode(c.body(f, tc.actor)), nil), http.StatusForbidden, tc.reason)
			if f.sa.calls != before {
				t.Fatalf("%s %s: a refused actor reached the verb", c.op, name)
			}
		}
		for _, actor := range []uuid.UUID{f.admin, f.owner, f.super} {
			if response := f.post(c.path, encode(c.body(f, actor)), nil); response.Code != http.StatusOK {
				t.Fatalf("%s by an allowed actor: %d %s", c.op, response.Code, response.Body)
			}
		}
	}
}

func TestKeyRouteActorRuleRunsAgainUnderTheLocks(t *testing.T) {
	f := newKeyFixture(t)
	f.users.inTx = &fakeProvUsers{states: f.users.states, roles: map[[2]uuid.UUID]string{{f.admin, f.org}: "viewer"}}
	for _, c := range keyCases()[:3] {
		expectRefusal(t, f.post(c.path, encode(c.body(f, f.admin)), nil), http.StatusForbidden, reasonAdminRequired)
	}
}

// A non-superuser may not issue or rotate on a superuser's account, at the precheck and under the locks. Revoke has no
// such check: the kill path stays open.
func TestKeyRouteSuperuserOwnedAccount(t *testing.T) {
	f := newKeyFixture(t)
	cases := keyCases()
	f.users.states[f.owner] = UserState{IsActive: true, IsSuperuser: true}
	for _, c := range cases[:2] {
		before := f.sa.calls
		expectRefusal(t, f.post(c.path, encode(c.body(f, f.admin)), nil), http.StatusForbidden, reasonSuperuserRequired)
		if f.sa.calls != before {
			t.Fatalf("%s: the precheck let a non-superuser through", c.op)
		}
		if response := f.post(c.path, encode(c.body(f, f.super)), nil); response.Code != http.StatusOK {
			t.Fatalf("%s by a superuser: %d %s", c.op, response.Code, response.Body)
		}
	}
	for _, c := range cases[2:] {
		if response := f.post(c.path, encode(c.body(f, f.admin)), nil); response.Code != http.StatusOK {
			t.Fatalf("%s by an admin on a superuser's account: %d %s", c.op, response.Code, response.Body)
		}
	}
	// The owner the verb locked is a superuser, the owner the precheck read is not.
	f.users.states[f.owner] = UserState{IsActive: true}
	promoted := map[uuid.UUID]UserState{}
	for k, v := range f.users.states {
		promoted[k] = v
	}
	promoted[f.owner] = UserState{IsActive: true, IsSuperuser: true}
	f.users.inTx = &fakeProvUsers{states: promoted, roles: f.users.roles}
	for _, c := range cases[:2] {
		before := f.sa.calls
		expectRefusal(t, f.post(c.path, encode(c.body(f, f.admin)), nil), http.StatusForbidden, reasonSuperuserRequired)
		if f.sa.calls != before+1 || f.sa.locked.Owner != f.owner {
			t.Fatalf("%s: the rule did not read the locked owner", c.op)
		}
	}
	if response := f.post(cases[2].path, encode(cases[2].body(f, f.admin)), nil); response.Code != http.StatusOK {
		t.Fatalf("revoke under the locks on a superuser's account: %d %s", response.Code, response.Body)
	}
}

// An unknown key answers a non-superuser as a key of another organization does; only a superuser is told it is unknown.
func TestKeyRouteUnknownKeyIsNamedOnlyToASuperuser(t *testing.T) {
	f := newKeyFixture(t)
	f.users.states[f.x] = UserState{IsActive: false, IsSuperuser: true}
	c := keyCases()[2]
	unknown := func(actor uuid.UUID) string {
		return encode(merged(c.body(f, actor), map[string]any{"credential_id": "crd_" + uuid.NewString()}))
	}
	foreign := serviceaccount.Info{ID: uuid.New(), OrgID: uuid.New(), Owner: uuid.New()}
	for name, actor := range map[string]uuid.UUID{"viewer": f.viewer, "admin of this org": f.admin, "stranger": f.target} {
		alike := f.post(c.path, unknown(actor), nil)
		home := f.sa.info
		f.sa.info = foreign
		f.sa.key.Principal = foreign.ID
		other := f.post(c.path, encode(c.body(f, actor)), nil)
		f.sa.info = home
		f.sa.key.Principal = home.ID
		if alike.Code != http.StatusForbidden || alike.Code != other.Code || reasonOf(t, alike) != reasonOf(t, other) {
			t.Fatalf("%s: unknown %d %s, foreign %d %s", name, alike.Code, alike.Body, other.Code, other.Body)
		}
	}
	expectRefusal(t, f.post(c.path, unknown(f.x), nil), http.StatusForbidden, reasonDeactivated)
	expectRefusal(t, f.post(c.path, unknown(f.super), nil), http.StatusNotFound, "key_not_found")
	// The key's account is gone: the same path.
	f.sa.key.Principal = uuid.New()
	expectRefusal(t, f.post(c.path, encode(c.body(f, f.super)), nil), http.StatusNotFound, "key_not_found")
	if f.sa.calls != 0 {
		t.Fatal("an unknown key reached a verb")
	}
}

func TestKeyRouteRevokeAnswerReadFailureAnswers503AndTheRepeatAnswersTheEntry(t *testing.T) {
	f := newKeyFixture(t)
	c := keyCases()[2]
	f.sa.afterErr = errors.New("read failed")
	expectRefusal(t, f.post(c.path, encode(c.body(f, f.admin)), nil), http.StatusServiceUnavailable, reasonStoreUnavailable)
	if f.sa.key.RevokedAt == nil {
		t.Fatal("the verb did not run")
	}
	f.sa.afterErr, f.sa.keyReads = nil, 0
	response := f.post(c.path, encode(c.body(f, f.admin)), nil)
	if _, err := contracts.ParseCredentialEntry(repoRoot(t), response.Body.Bytes(), time.Time{}); response.Code != http.StatusOK || err != nil {
		t.Fatalf("repeat after a failed answer read: %d %v", response.Code, err)
	}
}

func TestKeyRouteStatusFollowsTheTimestamps(t *testing.T) {
	now := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	at := now.Add(-time.Hour)
	reason := "compromised"
	for want, k := range map[string]serviceaccount.Key{
		"active":  {ExpiresAt: now.Add(time.Second)},
		"expired": {ExpiresAt: now},
		"revoked": {ExpiresAt: now.Add(-time.Second), RevokedAt: &at, RevokedReason: &reason},
	} {
		if got := keyBody(k, now).Status; got != want {
			t.Errorf("status %q, want %q", got, want)
		}
	}
}

func TestKeyRoutesTakeTheGoAPIWorkloadTokenAndNoOtherSubject(t *testing.T) {
	w := newWorkloadIssueFixture(t)
	f := &provWorkloadFixture{provFixture: newProvFixtureFor(t, docWithRow(t, PatternOrgSuspend), w.workload), w: w}
	want := uuidOfWire(t, f.w.subject)
	for _, c := range keyCases() {
		if !workloadOperations[c.op] {
			t.Fatalf("%s is not a workload operation", c.op)
		}
		header, _ := f.w.mint("workload+jwt", nil)
		if response := f.post(c.path, encode(c.body(f.provFixture, f.super)), header); response.Code != http.StatusOK {
			t.Fatalf("%s with the workload token: %d %s", c.op, response.Code, response.Body)
		}
		if c.op != opServiceAccountKeyList {
			if caller := f.sa.actor.Caller; caller.Workload == nil || *caller.Workload != want {
				t.Fatalf("%s: caller = %+v, want workload %s", c.op, caller, want)
			}
		}
		f.sa.key.RevokedAt, f.sa.key.RevokedReason = nil, nil
		before := f.sa.calls
		wrong := issuePreKeyRefusals()["wrong subject"](f.w)
		expectRefusal(t, f.post(c.path, encode(c.body(f.provFixture, f.super)), wrong), http.StatusUnauthorized, reasonInvalidCredentials)
		if f.sa.calls != before {
			t.Fatalf("%s: a token of another subject reached the verb", c.op)
		}
	}
}

// The key routes are on the startup gate: a build whose document has no row for one of them builds no route, and the
// embedded document serves all four.
func TestKeyRoutesAreOnTheStartupGate(t *testing.T) {
	f := newProvFixture(t)
	for _, c := range keyCases() {
		if !slices.Contains(provisioningPatterns, c.path) {
			t.Errorf("%s is not on the startup gate", c.path)
		}
		if provisioningRouteOps[c.op] != c.path {
			t.Errorf("%s is not in the route operations", c.op)
		}
		err := provisioningRowsPresent(embeddedDocWithoutRow(t, c.path), false)
		if !errors.Is(err, errProvisioningRows) || !strings.Contains(err.Error(), "POST "+c.path+" ") {
			t.Errorf("a document without the %s row: %v, want a refusal naming the route", c.path, err)
		}
		if response := f.post(c.path, encode(c.body(f, f.super)), nil); response.Code != http.StatusOK {
			t.Errorf("%s on the embedded document: %d, want 200", c.path, response.Code)
		}
		f.sa.key.RevokedAt, f.sa.key.RevokedReason = nil, nil
	}
}

func TestKeyBodyCarriesTheRowFingerprint(t *testing.T) {
	raw, err := json.Marshal(keyBody(serviceaccount.Key{Fingerprint: "0123456789abcdef", ExpiresAt: time.Now().Add(time.Hour)}, time.Now()))
	if err != nil || !strings.Contains(string(raw), `"fingerprint":"0123456789abcdef"`) {
		t.Fatalf("%s %v", raw, err)
	}
}
