package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/keyrotation"
	"github.com/full-chaos/dev-health-ops/internal/auth/restorefence"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

const execTestPassword = "s3cr3t-pw-do-not-print"

type stubStore struct {
	calls          []string
	roleErr        error
	timeline       int64
	timelineErr    error
	checkErr       error
	manifest       restorefence.Manifest
	applied        *restorefence.Manifest
	applyOpts      restorefence.Options
	applyErr       error
	initOpts       restorefence.InitOptions
	initReport     restorefence.InitReport
	initErr        error
	actions        []restorefence.OperatorAction
	reportSections []restorefence.SectionReport
	closed         bool
}

func (s *stubStore) CheckNotRuntimeRole(context.Context, string) error {
	s.calls = append(s.calls, "role")
	return s.roleErr
}
func (s *stubStore) Timeline(context.Context) (int64, error) {
	s.calls = append(s.calls, "timeline")
	return s.timeline, s.timelineErr
}
func (s *stubStore) Check(context.Context, authschema.ValidatedIdentifier) error {
	s.calls = append(s.calls, "check")
	return s.checkErr
}
func (s *stubStore) Export(context.Context, authschema.ValidatedIdentifier) (restorefence.Manifest, error) {
	s.calls = append(s.calls, "export")
	return s.manifest, nil
}
func (s *stubStore) Apply(_ context.Context, _ authschema.ValidatedIdentifier, m *restorefence.Manifest, o restorefence.Options) (restorefence.Report, error) {
	s.calls = append(s.calls, "apply")
	s.applied, s.applyOpts = m, o
	src := "manifest"
	if m == nil {
		src = "blind_margin"
	}
	return restorefence.Report{Timeline: s.timeline, Source: src, DryRun: !o.Apply, Actions: s.actions, Sections: s.reportSections}, s.applyErr
}
func (s *stubStore) Init(_ context.Context, _ authschema.ValidatedIdentifier, o restorefence.InitOptions) (restorefence.InitReport, error) {
	s.calls = append(s.calls, "init")
	s.initOpts = o
	return s.initReport, s.initErr
}
func (s *stubStore) PendingActions(context.Context, authschema.ValidatedIdentifier) ([]restorefence.OperatorAction, error) {
	s.calls = append(s.calls, "actions")
	return s.actions, nil
}
func (s *stubStore) Close() { s.closed = true }

func runExec(t *testing.T, store *stubStore, openErr error, args ...string) (rc int, stdout, stderr string) {
	t.Helper()
	prev := openRestoreFenceStore
	t.Cleanup(func() { openRestoreFenceStore = prev })
	openRestoreFenceStore = func(context.Context, string, io.Writer) (restoreFenceStore, error) {
		if openErr != nil {
			return nil, openErr
		}
		return store, nil
	}
	env := map[string]string{
		envMigrationURI: "host=db port=5432 user=postgres password='" + execTestPassword + "' dbname=postgres sslmode=disable",
		envRuntimeRole:  "devhealth_auth",
	}
	lookup := secrets.LookupEnv(func(k string) (string, bool) { v, ok := env[k]; return v, ok })
	var out, errb bytes.Buffer
	rc = executeRestoreFence(context.Background(), args, lookup, &out, &errb)
	return rc, out.String(), errb.String()
}

func noSecret(t *testing.T, outputs ...string) {
	t.Helper()
	for _, o := range outputs {
		if strings.Contains(o, execTestPassword) || strings.Contains(o, "host=db") {
			t.Errorf("the DSN or its password reached the output: %q", o)
		}
	}
}

func TestExecStatusOnAnUnfencedTimelineIsNotReady(t *testing.T) {
	store := &stubStore{timeline: 2, checkErr: restorefence.ErrNotFenced}
	rc, out, errOut := runExec(t, store, nil, "status")
	if rc != 1 || !strings.Contains(out, "restore-fence status timeline=2 fenced=false") {
		t.Fatalf("rc %d, stdout %q", rc, out)
	}
	if !store.closed {
		t.Error("the store was not closed")
	}
	noSecret(t, out, errOut)
}

func TestExecStatusOnAFencedTimelinePrintsTheActions(t *testing.T) {
	store := &stubStore{timeline: 1, actions: []restorefence.OperatorAction{{Kind: restorefence.ActionForcePasswordReset, Source: "public.users", IDs: []string{"u1"}}}}
	rc, out, _ := runExec(t, store, nil, "status")
	if rc != 0 || !strings.Contains(out, "fenced=true") || !strings.Contains(out, "operator-action force_password_reset source=public.users count=1 ids=u1") {
		t.Fatalf("rc %d, stdout %q", rc, out)
	}
}

func TestExecRefusesToRunAsTheRuntimeRoleBeforeAnyVerb(t *testing.T) {
	store := &stubStore{roleErr: keyrotation.ErrRuntimeRole}
	rc, out, errOut := runExec(t, store, nil, "init")
	if rc != 1 || !strings.Contains(errOut, "runtime role") || out != "" {
		t.Fatalf("rc %d, stdout %q, stderr %q", rc, out, errOut)
	}
	if len(store.calls) != 1 || store.calls[0] != "role" {
		t.Errorf("calls after the refusal: %v", store.calls)
	}
	noSecret(t, out, errOut)
}

func TestExecReportsAnUnavailableEndpointWithoutTheDSN(t *testing.T) {
	rc, out, errOut := runExec(t, &stubStore{}, errors.New("dial host=db password="+execTestPassword), "status")
	if rc != 1 || !strings.Contains(errOut, "migration endpoint unavailable") {
		t.Fatalf("rc %d, stderr %q", rc, errOut)
	}
	noSecret(t, out, errOut)
}

func TestExecExportWritesAnExclusiveOwnerOnlyFile(t *testing.T) {
	m := restorefence.Manifest{Version: restorefence.ManifestVersion, SourceTimeline: 2, Floors: []restorefence.Floor{}, Sections: []restorefence.Section{}}
	if err := m.Seal(); err != nil {
		t.Fatal(err)
	}
	store := &stubStore{manifest: m}
	path := filepath.Join(t.TempDir(), "m.json")
	rc, out, errOut := runExec(t, store, nil, "export", "-out", path)
	if rc != 0 || !strings.Contains(out, "restore-fence export timeline=2 floors=0 sections=0 sha256="+m.SHA256) {
		t.Fatalf("rc %d, stdout %q, stderr %q", rc, out, errOut)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("manifest file: %v %v", info, err)
	}
	if rc2, _, err2 := runExec(t, store, nil, "export", "-out", path); rc2 != 1 || !strings.Contains(err2, "must not exist") {
		t.Errorf("a second export over the file: rc %d, stderr %q", rc2, err2)
	}
}

func TestExecApplyIsADryRunUnlessApplyAndPassesTheManifest(t *testing.T) {
	m := restorefence.Manifest{Version: restorefence.ManifestVersion, SourceTimeline: 2, Floors: []restorefence.Floor{}, Sections: []restorefence.Section{}}
	if err := m.Seal(); err != nil {
		t.Fatal(err)
	}
	raw, err := m.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "m.json")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	store := &stubStore{timeline: 2, actions: []restorefence.OperatorAction{{Kind: restorefence.ActionUserTombstoned, Source: "public.users.ids", IDs: []string{"u9"}}}}
	rc, out, _ := runExec(t, store, nil, "apply", "-manifest", path)
	if rc != 0 || store.applyOpts.Apply || store.applied == nil || store.applied.SHA256 != m.SHA256 || !strings.Contains(out, `"dry_run":true`) {
		t.Fatalf("dry run: rc %d, opts %+v, stdout %q", rc, store.applyOpts, out)
	}
	if !strings.Contains(out, "operator-action user_tombstoned source=public.users.ids count=1 ids=u9") {
		t.Errorf("the action line is missing: %q", out)
	}
	store = &stubStore{timeline: 2}
	if rc, _, _ := runExec(t, store, nil, "apply", "-manifest", path, "-apply"); rc != 0 || !store.applyOpts.Apply {
		t.Errorf("-apply did not reach the store: rc %d opts %+v", rc, store.applyOpts)
	}
	store = &stubStore{applyErr: errors.New("boom")}
	if rc, _, errOut := runExec(t, store, nil, "apply", "-manifest", path); rc != 1 || !strings.Contains(errOut, "restore-fence error") {
		t.Errorf("a failed apply: rc %d stderr %q", rc, errOut)
	}
	if rc, _, errOut := runExec(t, &stubStore{}, nil, "apply", "-manifest", filepath.Join(t.TempDir(), "nope.json")); rc != 1 || !strings.Contains(errOut, "could not read the manifest file") {
		t.Errorf("a missing manifest file: rc %d stderr %q", rc, errOut)
	}
}

func TestExecInitPassesItsOptionsAndPrintsTheTimelineOnly(t *testing.T) {
	store := &stubStore{initReport: restorefence.InitReport{Timeline: 3}}
	rc, out, _ := runExec(t, store, nil, "init", "-timeline", "3")
	if rc != 0 || store.initOpts.Timeline != 3 || store.initOpts.IfAbsent || !strings.Contains(out, "restore-fence init timeline=3 written=true") {
		t.Fatalf("rc %d, opts %+v, stdout %q", rc, store.initOpts, out)
	}
	store = &stubStore{initReport: restorefence.InitReport{Timeline: 1, AlreadyFenced: true}}
	rc, out, _ = runExec(t, store, nil, "init", "-if-absent")
	if rc != 0 || !store.initOpts.IfAbsent || store.initOpts.Timeline != 0 || !strings.Contains(out, "already_fenced=true written=false") {
		t.Fatalf("-if-absent: rc %d, opts %+v, stdout %q", rc, store.initOpts, out)
	}
	store = &stubStore{initErr: restorefence.ErrInitRefused}
	if rc, _, errOut := runExec(t, store, nil, "init"); rc != 1 || !strings.Contains(errOut, "restore-fence error") {
		t.Errorf("a refused init: rc %d stderr %q", rc, errOut)
	}
}

func TestExecRefusesBadArgumentsBeforeConnecting(t *testing.T) {
	store := &stubStore{}
	rc, _, errOut := runExec(t, store, nil, "init", "-if-absent", "-timeline", "2")
	if rc != 2 || !strings.Contains(errOut, "never names a timeline") || len(store.calls) != 0 {
		t.Fatalf("rc %d, stderr %q, calls %v", rc, errOut, store.calls)
	}
}

// The seam is for tests only: the shipped default must be the real, database-backed opener.
func TestTheDefaultStoreOpenerIsTheRealOne(t *testing.T) {
	if reflect.ValueOf(openRestoreFenceStore).Pointer() != reflect.ValueOf(openPGRestoreFenceStore).Pointer() {
		t.Fatal("openRestoreFenceStore is not the real PostgreSQL opener: a test seam was left swapped")
	}
}

// D6459 P2-a: a blind margin (no manifest) removes every legacy reset and verification token and every unconsumed
// link credential, fail closed; the apply output prints those counts, and the help says so.
func TestExecBlindMarginApplyPrintsWhatItRemoved(t *testing.T) {
	store := &stubStore{timeline: 2}
	store.reportSections = []restorefence.SectionReport{
		{Name: "public.password_reset_tokens", Changed: 3}, {Name: "public.email_verification_tokens", Changed: 2},
		{Name: "auth.link_credentials.ids", Changed: 4}, {Name: "auth.invites.ids", Changed: 6}, {Name: "public.memberships", Changed: 9},
	}
	rc, out, _ := runExec(t, store, nil, "apply", "-blind-margin", "1000")
	if rc != 0 || !strings.Contains(out, "restore-fence blind-margin removed legacy_tokens=5 link_credentials=4 invites=6") {
		t.Fatalf("rc %d, stdout %q", rc, out)
	}
	if !strings.Contains(restoreFenceUsage, "every legacy reset and verification token") || !strings.Contains(restoreFenceUsage, "unconsumed link credential") || !strings.Contains(restoreFenceUsage, "every pending invite") {
		t.Errorf("the usage does not say what a blind margin removes: %s", restoreFenceUsage)
	}
}

// 11878 (1) and the Q1 ruling: the blind-margin line counts the legacy invites it revoked, and the help says that a
// blind margin leaves the legacy access flags as restored and lists every active row (ids only) for review.
func TestExecBlindMarginApplyCountsLegacyInvitesAndListsTheFlagsForReview(t *testing.T) {
	store := &stubStore{timeline: 2}
	store.reportSections = []restorefence.SectionReport{{Name: "auth.invites.ids", Changed: 6}, {Name: "public.org_invites.ids", Changed: 2}}
	store.actions = []restorefence.OperatorAction{{Kind: "review_after_blind_margin", Source: "public.organizations", IDs: []string{"11111111-1111-1111-1111-111111111111"}}}
	rc, out, _ := runExec(t, store, nil, "apply", "-blind-margin", "1000")
	if rc != 0 || !strings.Contains(out, "invites=6 legacy_invites=2") {
		t.Fatalf("rc %d, stdout %q", rc, out)
	}
	if !strings.Contains(out, "operator-action review_after_blind_margin source=public.organizations count=1 ids=11111111-1111-1111-1111-111111111111") {
		t.Errorf("the review list is not printed: %q", out)
	}
	for _, want := range []string{"leaves the legacy access flags", "review_after_blind_margin"} {
		if !strings.Contains(restoreFenceUsage, want) {
			t.Errorf("the usage lacks %q: %s", want, restoreFenceUsage)
		}
	}
}
