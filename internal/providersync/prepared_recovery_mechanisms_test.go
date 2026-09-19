package providersync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// countingEffectsFactory builds sinks from a credential the way a
// credential-bound route does, and counts the builds.
type countingEffectsFactory struct {
	builds   int
	sink     EffectSink
	readback EffectReadback
	err      error
}

func (factory *countingEffectsFactory) build(providerfoundation.Credential) (EffectSink, EffectReadback, error) {
	factory.builds++
	return factory.sink, factory.readback, factory.err
}

type failingCredentialRepository struct {
	calls int
	err   error
}

func (repository *failingCredentialRepository) ResolveEncrypted(
	context.Context, providerfoundation.TenantScope,
) (providerfoundation.EncryptedCredential, error) {
	repository.calls++
	return providerfoundation.EncryptedCredential{}, repository.err
}

// runFactoryRoute executes github/deployments with its sinks built by a
// factory, either as a first attempt (no ledger) or as the replay of a stored
// snapshot, and reports what happened.
func runFactoryRoute(
	t *testing.T, replay bool, credentials providerfoundation.CredentialRepository,
	factory *countingEffectsFactory,
) (error, *staticCompleteRouteHandler, *trackingCompleteRouteDoer) {
	t.Helper()
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	claim, session := preparedWorkItemsSession(t, now, "github", "deployments")
	batch := preparedDeploymentsBatch(t, claim)
	ledger := &memoryEffectLedger{}
	if replay {
		if _, err := ledger.PrepareRouteSnapshot(context.Background(), claim, batch, ShadowComparison{Match: true}, now); err != nil {
			t.Fatal(err)
		}
	}
	handler := &staticCompleteRouteHandler{batch: batch}
	executor := preparedGitHubExecutor(now, handler, ledger, nil)
	executor.Committer.Sink, executor.Committer.Readback = nil, nil
	executor.EffectsFactory = factory.build
	executor.Credentials.Repository = credentials
	doer := &trackingCompleteRouteDoer{}
	executor.Doer = doer
	descriptor := preparedDeploymentsDescriptor(t)
	_, err := executor.Execute(context.Background(), session, descriptor)
	return err, handler, doer
}

// TestSnapshotReplayBindsFactorySinksLikeAFirstAttempt is the early-binding
// table: a replay of a route whose sinks come from its credential resolves
// the credential and builds the sinks before it commits -- once, with no
// provider request -- and every failure of that binding is the same error a
// first attempt gets from the same failure.
func TestSnapshotReplayBindsFactorySinksLikeAFirstAttempt(t *testing.T) {
	resolveFailure := errors.New("credential store unavailable")
	factoryFailure := errors.New("clickhouse connection refused")
	for _, cell := range []struct {
		name        string
		resolveErr  error
		factoryErr  error
		nilSink     bool
		nilReadback bool
		wantErr     error
		wantBuilds  int
	}{
		{name: "binds and replays", wantBuilds: 1},
		{name: "credential resolve fails", resolveErr: resolveFailure, wantErr: resolveFailure, wantBuilds: 0},
		{name: "factory fails", factoryErr: factoryFailure, wantErr: factoryFailure, wantBuilds: 1},
		{name: "factory builds no sink", nilSink: true, wantErr: ErrInvalidConfiguration, wantBuilds: 1},
		{name: "factory builds a sink without a readback", nilReadback: true, wantErr: ErrInvalidConfiguration, wantBuilds: 1},
	} {
		t.Run(cell.name, func(t *testing.T) {
			outcomes := map[bool]error{}
			for _, replay := range []bool{false, true} {
				var credentials providerfoundation.CredentialRepository = &trackingCompleteRouteCredentialRepository{provider: "github"}
				if cell.resolveErr != nil {
					credentials = &failingCredentialRepository{err: cell.resolveErr}
				}
				factory := &countingEffectsFactory{sink: &memoryEffectSink{}, readback: staticEffectReadback{}, err: cell.factoryErr}
				if cell.nilSink {
					factory.sink = nil
				}
				if cell.nilReadback {
					factory.readback = nil
				}
				err, handler, doer := runFactoryRoute(t, replay, credentials, factory)
				outcomes[replay] = err
				if factory.builds != cell.wantBuilds {
					t.Fatalf("replay=%v builds=%d want %d", replay, factory.builds, cell.wantBuilds)
				}
				if tracking, ok := credentials.(*trackingCompleteRouteCredentialRepository); ok && tracking.resolves != 1 {
					t.Fatalf("replay=%v credential resolves=%d want exactly 1", replay, tracking.resolves)
				}
				if replay && (doer.requests != 0 || !handler.normalizedAt.IsZero()) {
					t.Fatalf("replay made %d provider requests, re-collected=%v", doer.requests, !handler.normalizedAt.IsZero())
				}
				if cell.wantErr == nil && err != nil {
					t.Fatalf("replay=%v err=%v", replay, err)
				}
				if cell.wantErr != nil && !errors.Is(err, cell.wantErr) {
					t.Fatalf("replay=%v err=%v want %v", replay, err, cell.wantErr)
				}
			}
			if (outcomes[false] == nil) != (outcomes[true] == nil) ||
				(outcomes[false] != nil && outcomes[false].Error() != outcomes[true].Error()) {
				t.Fatalf("first attempt err=%v replay err=%v: a binding failure must look the same on both", outcomes[false], outcomes[true])
			}
		})
	}
}

// TestSnapshotReplayWithoutAFactoryResolvesNoCredential keeps replay of a
// route with static sinks free of credential access.
func TestSnapshotReplayWithoutAFactoryResolvesNoCredential(t *testing.T) {
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	claim, session := preparedWorkItemsSession(t, now, "github", "deployments")
	batch := preparedDeploymentsBatch(t, claim)
	ledger := &memoryEffectLedger{}
	if _, err := ledger.PrepareRouteSnapshot(context.Background(), claim, batch, ShadowComparison{Match: true}, now); err != nil {
		t.Fatal(err)
	}
	credentials := &forbiddenCredentialRepository{}
	executor := preparedGitHubExecutor(now, &staticCompleteRouteHandler{batch: batch}, ledger, &memoryEffectSink{})
	executor.Credentials.Repository = credentials
	if _, err := executor.Execute(context.Background(), session, preparedDeploymentsDescriptor(t)); err != nil || credentials.calls != 0 {
		t.Fatalf("err=%v credential calls=%d, want a replay without credential access", err, credentials.calls)
	}
}

// earlierSnapshotType builds, from the vendored source of the binary before
// worklog observations (testdata/earlier_binary), the stored-snapshot type its
// strict reader decodes into: every field, type and json tag as that source
// declares them, so the test decodes with that binary's exact shape.
func earlierSnapshotType(t *testing.T) reflect.Type {
	t.Helper()
	parsed, err := parser.ParseFile(token.NewFileSet(), "testdata/earlier_binary/prepared_route_snapshot.go.txt", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	structs := map[string]*ast.StructType{}
	for _, declaration := range parsed.Decls {
		general, ok := declaration.(*ast.GenDecl)
		if !ok {
			continue
		}
		for _, spec := range general.Specs {
			if typeSpec, ok := spec.(*ast.TypeSpec); ok {
				if structType, ok := typeSpec.Type.(*ast.StructType); ok {
					structs[typeSpec.Name.Name] = structType
				}
			}
		}
	}
	known := map[string]reflect.Type{
		"string": reflect.TypeOf(""), "int": reflect.TypeOf(0), "time.Time": reflect.TypeOf(time.Time{}),
		"*time.Time": reflect.TypeOf((*time.Time)(nil)), "json.RawMessage": reflect.TypeOf(json.RawMessage(nil)),
		"[]json.RawMessage": reflect.TypeOf([]json.RawMessage(nil)), "FetchEvidence": reflect.TypeOf(FetchEvidence{}),
		"ShadowComparison": reflect.TypeOf(ShadowComparison{}), "EffectRecoveryPolicy": reflect.TypeOf(EffectRecoveryPolicy("")),
	}
	var build func(name string) reflect.Type
	build = func(name string) reflect.Type {
		structType, ok := structs[name]
		if !ok {
			t.Fatalf("vendored source declares no struct %s", name)
		}
		var fields []reflect.StructField
		for _, field := range structType.Fields.List {
			expression := types.ExprString(field.Type)
			fieldType, ok := known[expression]
			if !ok && strings.HasPrefix(expression, "[]") {
				fieldType = reflect.SliceOf(build(strings.TrimPrefix(expression, "[]")))
				ok = true
			}
			if !ok {
				t.Fatalf("vendored field type %s is not mapped", expression)
			}
			tag := ""
			if field.Tag != nil {
				tag = strings.Trim(field.Tag.Value, "`")
			}
			for _, fieldName := range field.Names {
				fields = append(fields, reflect.StructField{Name: fieldName.Name, Type: fieldType, Tag: reflect.StructTag(tag)})
			}
		}
		return reflect.StructOf(fields)
	}
	return build("storedPreparedRouteSnapshot")
}

// TestSnapshotWorklogObservationsAcrossBinaries is the rollback cell, executed:
// a snapshot written with observations decodes under the earlier binary's
// strict reader (its effects replay; the observations stay an unknown result
// key it ignores), and this binary's decode restores them and removes the key.
func TestSnapshotWorklogObservationsAcrossBinaries(t *testing.T) {
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	claim, _ := preparedWorkItemsSession(t, now, "github", "deployments")
	batch := preparedDeploymentsBatch(t, claim)
	batch.WorklogObservations = []JiraWorklogFetchObservation{{IssueKey: "ABC-123", GraphQLAttempted: true, GraphQLRequests: 2, RESTFallbackUsed: true, RESTRequests: 1}}
	payload, reference, err := encodePreparedRouteManifest(claim, batch, ShadowComparison{Match: true}, now)
	if err != nil {
		t.Fatal(err)
	}

	earlier := json.NewDecoder(bytes.NewReader(payload))
	earlier.DisallowUnknownFields()
	decoded := reflect.New(earlierSnapshotType(t))
	if err := earlier.Decode(decoded.Interface()); err != nil {
		t.Fatalf("the earlier binary's strict reader refuses the new snapshot: %v", err)
	}
	old := decoded.Elem()
	var oldResult map[string]any
	effects := old.FieldByName("Effects")
	if err := json.Unmarshal(old.FieldByName("Result").Bytes(), &oldResult); err != nil || effects.Len() != 1 {
		t.Fatalf("earlier reader result=%v effects=%d err=%v", oldResult, effects.Len(), err)
	}
	if _, carried := oldResult[preparedSnapshotWorklogResultKey]; !carried {
		t.Fatal("the earlier reader does not see the observations as an ordinary result key")
	}
	for index := range effects.Len() {
		effect := effects.Index(index)
		rows := effect.FieldByName("Rows").Interface().([]json.RawMessage)
		rebuilt, err := BuildEffectBatch(effect.FieldByName("Destination").String(),
			EffectRecoveryPolicy(effect.FieldByName("Recovery").String()), rows)
		if err != nil || rebuilt.ContentDigest != effect.FieldByName("ContentDigest").String() {
			t.Fatalf("earlier reader cannot rebuild the stored effect: err=%v", err)
		}
	}

	state, err := NewEffectLedgerState(claim, batch.Effects, now)
	if err != nil {
		t.Fatal(err)
	}
	state.SchemaVersion, state.PreparedSnapshot = "v2", &reference
	manifest, err := decodePreparedRouteManifest(payload, claim, state)
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.Batch.WorklogObservations) != 1 || manifest.Batch.WorklogObservations[0] != batch.WorklogObservations[0] {
		t.Fatalf("replayed worklog observations=%+v want %+v", manifest.Batch.WorklogObservations, batch.WorklogObservations)
	}
	if _, leaked := manifest.Batch.Result[preparedSnapshotWorklogResultKey]; leaked {
		t.Fatal("the reserved key stayed in the replayed result")
	}
	collision := preparedDeploymentsBatch(t, claim)
	collision.Result[preparedSnapshotWorklogResultKey] = "route value"
	if _, _, err := encodePreparedRouteManifest(claim, collision, ShadowComparison{Match: true}, now); !errors.Is(err, ErrEffectRecoveryUnsafe) {
		t.Fatalf("a route result using the reserved key err=%v, want a refusal", err)
	}
}

// TestSnapshotWrittenBeforeWorklogObservationsStillDecodes pins the captured
// payload and ledger an earlier binary wrote (testdata/prepared_snapshots),
// and that a batch without observations encodes to the same bytes today.
func TestSnapshotWrittenBeforeWorklogObservationsStillDecodes(t *testing.T) {
	payload, err := os.ReadFile("testdata/prepared_snapshots/github_deployments_payload.json")
	if err != nil {
		t.Fatal(err)
	}
	ledgerBytes, err := os.ReadFile("testdata/prepared_snapshots/github_deployments_ledger.json")
	if err != nil {
		t.Fatal(err)
	}
	state, err := decodeEffectLedgerState(ledgerBytes)
	if err != nil {
		t.Fatal(err)
	}
	claim := githubWorkItemOracleClaim()
	claim.Dataset = "deployments"
	manifest, err := decodePreparedRouteManifest(payload, claim, state)
	if err != nil {
		t.Fatalf("captured earlier payload no longer decodes: %v", err)
	}
	if len(manifest.Batch.WorklogObservations) != 0 || len(manifest.Batch.Effects) != 1 {
		t.Fatalf("decoded manifest=%+v", manifest.Batch)
	}
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	encoded, _, err := encodePreparedRouteManifest(claim, preparedDeploymentsBatch(t, claim), ShadowComparison{Match: true}, now)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encoded, payload) {
		t.Fatalf("a batch without observations encodes differently from the earlier payload:\nnow:    %s\nbefore: %s", encoded, payload)
	}
}

func TestSnapshotRefusesASensitiveKeyByNameOnly(t *testing.T) {
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	claim, _ := preparedWorkItemsSession(t, now, "github", "deployments")
	for key, canonical := range map[string]string{
		"authorization": "authorization", "credential": "credential", "credentials": "credential",
		"token": "token", "headers": "header", "source_metadata": "source_metadata",
		"integration_config": "integration_config", "ciphertext": "ciphertext",
		"raw_payload": "raw_payload", "response_body": "response_body",
	} {
		for _, spelling := range []string{key, strings.ToUpper(key), strings.ReplaceAll(key, "_", "-")} {
			effect, err := effectBatchFromValues("deployments", EffectReadbackRequired,
				[]map[string]any{{"meta": map[string]any{"fields": []any{map[string]any{spelling: "value-must-not-leak"}}}}})
			if err != nil {
				t.Fatal(err)
			}
			batch := preparedDeploymentsBatch(t, claim)
			batch.Effects = []EffectBatch{effect}
			_, _, err = encodePreparedRouteManifest(claim, batch, ShadowComparison{Match: true}, now)
			var named preparedSnapshotSensitiveKeyError
			if !errors.Is(err, ErrPreparedRouteSnapshotSensitiveKey) || !errors.Is(err, ErrEffectRecoveryUnsafe) ||
				!errors.As(err, &named) || named.key != canonical || strings.Contains(err.Error(), "value-must-not-leak") {
				t.Fatalf("key %q err=%v named=%q", spelling, err, named.key)
			}
		}
	}
	batch := preparedDeploymentsBatch(t, claim)
	batch.Result = map[string]any{"deployments_synced": 1, "detail": map[string]any{"Token": "value-must-not-leak"}}
	// The result is route-built: a protected key there refuses outright and
	// takes no fallback.
	if _, _, err := encodePreparedRouteManifest(claim, batch, ShadowComparison{Match: true}, now); !errors.Is(err, ErrEffectRecoveryUnsafe) ||
		errors.Is(err, ErrPreparedRouteSnapshotSensitiveKey) {
		t.Fatalf("a sensitive key in the result err=%v, want a plain ErrEffectRecoveryUnsafe refusal", err)
	}
}

func TestSensitiveKeyBatchCommitsWithoutASnapshotAndSaysSo(t *testing.T) {
	log := captureSlog(t)
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	claim, session := preparedWorkItemsSession(t, now, "github", "deployments")
	effect, err := effectBatchFromValues("deployments", EffectReadbackRequired,
		[]map[string]any{{"meta": map[string]any{"headers": "value-must-not-leak"}}})
	if err != nil {
		t.Fatal(err)
	}
	batch := preparedDeploymentsBatch(t, claim)
	batch.Effects = []EffectBatch{effect}
	ledger := &memoryEffectLedger{}
	sink := &memoryEffectSink{}
	result, err := preparedGitHubExecutor(now, &staticCompleteRouteHandler{batch: batch}, ledger, sink).
		Execute(context.Background(), session, preparedDeploymentsDescriptor(t))
	if err != nil || result.Effects.Written != 1 || ledger.preparedPrepares != 0 || ledger.state.SchemaVersion == "v2" {
		t.Fatalf("err=%v result=%+v prepares=%d schema=%q, want a commit without a snapshot", err, result.Effects, ledger.preparedPrepares, ledger.state.SchemaVersion)
	}
	if result.Result["recovery"] != "recollect" {
		t.Fatalf("unit result recovery=%v want recollect", result.Result["recovery"])
	}
	for _, want := range []string{"provider_sync.prepared_snapshot_sensitive_key_fallback", "key=header", "recovery=recollect"} {
		if !strings.Contains(log.String(), want) {
			t.Fatalf("log lacks %q: %s", want, log.String())
		}
	}
	if strings.Contains(log.String(), "value-must-not-leak") {
		t.Fatalf("log carries the value of the sensitive key: %s", log.String())
	}
}

func TestWorkItemsStillRefusesASensitiveKeySnapshot(t *testing.T) {
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	claim, session := preparedGitHubWorkItemsSession(t, now)
	batch := preparedGitHubWorkItemsFixture(t, claim)
	first := batch.Effects[0]
	extra, err := json.Marshal(map[string]any{"token": "value-must-not-leak"})
	if err != nil {
		t.Fatal(err)
	}
	rebuilt, err := BuildEffectBatch(first.Destination, first.Recovery, append(append([]json.RawMessage(nil), first.Rows...), extra))
	if err != nil {
		t.Fatal(err)
	}
	batch.Effects[0] = rebuilt
	descriptor, _ := Descriptor("github", "work-items")
	sink := &memoryEffectSink{}
	_, err = preparedGitHubExecutor(now, &staticCompleteRouteHandler{batch: batch}, &memoryEffectLedger{}, sink).
		Execute(context.Background(), session, descriptor)
	if !errors.Is(err, ErrPreparedRouteSnapshotSensitiveKey) || len(sink.destinations) != 0 {
		t.Fatalf("err=%v writes=%v, want the refusal with nothing written", err, sink.destinations)
	}
}

// TestSupersededDiscardWithFactorySinksResolvesTheCredentialOnce covers the
// one attempt that needs the credential twice: the discard's readback binds
// the factory sinks early, and the re-collect that follows reuses them.
func TestSupersededDiscardWithFactorySinksResolvesTheCredentialOnce(t *testing.T) {
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	claim, session := preparedWorkItemsSession(t, now, "github", "prs")
	ledger := seedSupersededPullRequestSnapshot(t, claim, now, map[string]supersededEffectSeed{
		supersededSecondDestination: {status: GenerationBlockWriting, readback: discardReadbackResult{inspection: EffectAbsent}},
	})
	readback := &tableReadback{results: map[string]discardReadbackResult{supersededSecondDestination: {inspection: EffectAbsent}}}
	factory := &countingEffectsFactory{sink: &memoryEffectSink{}, readback: readback}
	credentials := &trackingCompleteRouteCredentialRepository{provider: "github"}
	handler := &staticCompleteRouteHandler{batch: currentPullRequestSocialBatch(t, claim)}
	executor := preparedGitHubExecutor(now.Add(time.Hour), handler, ledger, nil)
	executor.Committer.Sink, executor.Committer.Readback = nil, nil
	executor.EffectsFactory = factory.build
	executor.Credentials.Repository = credentials
	descriptor, _ := Descriptor("github", "prs")
	if _, err := executor.Execute(context.Background(), session, descriptor); err != nil {
		t.Fatal(err)
	}
	if credentials.resolves != 1 || factory.builds != 1 || readback.calls != 1 || ledger.resets != 1 || handler.normalizedAt.IsZero() {
		t.Fatalf("resolves=%d builds=%d readbacks=%d resets=%d recollected=%v, want one binding serving the discard readback and the re-collect",
			credentials.resolves, factory.builds, readback.calls, ledger.resets, !handler.normalizedAt.IsZero())
	}
}

// TestRouteCompletedLogFieldsAreOwnScalars pins the completion line's
// record: identity strings the package sets from the claim and its own
// recovery word, integer counts, and "*_synced" counts. A field of any other
// kind -- a string that could come from provider content, a map, a nested
// value -- fails here until it is argued onto this list.
func TestRouteCompletedLogFieldsAreOwnScalars(t *testing.T) {
	ownStrings := map[string]bool{"Provider": true, "Dataset": true, "Unit": true, "Recovery": true}
	record := reflect.TypeOf(routeCompletedLog{})
	for index := range record.NumField() {
		field := record.Field(index)
		switch {
		case field.Type.Kind() == reflect.String && ownStrings[field.Name]:
		case field.Type.Kind() == reflect.Int:
		case field.Name == "Counts" && field.Type == reflect.TypeOf([]routeCompletedCount(nil)):
		default:
			t.Errorf("completion-line field %s has type %s: not an own scalar", field.Name, field.Type)
		}
	}
	count := reflect.TypeOf(routeCompletedCount{})
	if count.NumField() != 2 || count.Field(0).Type.Kind() != reflect.String || count.Field(1).Type.Kind() != reflect.Int64 {
		t.Fatalf("routeCompletedCount=%v want {Name string; Value int64}", count)
	}
}

// TestRouteCompletedLineCarriesNoProviderContent executes a route whose
// result mixes counts with provider content in every shape and reads the
// completion line.
func TestRouteCompletedLineCarriesNoProviderContent(t *testing.T) {
	log := captureSlog(t)
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	claim, session := preparedWorkItemsSession(t, now, "github", "deployments")
	batch := preparedDeploymentsBatch(t, claim)
	batch.Result = map[string]any{
		"deployments_synced": 3,
		"big_synced":         json.Number("9007199254740993"),
		"repo":               "provider-repo-name",
		"detail":             map[string]any{"token": "provider-secret"},
		"note":               `{"token":"provider-secret"}`,
		"raw":                json.RawMessage(`"{\"token\":\"provider-secret\"}"`),
		"label_synced":       "provider-label",
		"ratio_synced":       0.5,
		"typed":              map[string]string{"headers": "provider-secret"},
		"other_total":        7,
	}
	descriptor := preparedDeploymentsDescriptor(t)
	descriptor.PreparedManifestRecovery = false
	if _, err := preparedGitHubExecutor(now, &staticCompleteRouteHandler{batch: batch}, &memoryEffectLedger{}, &memoryEffectSink{}).
		Execute(context.Background(), session, descriptor); err != nil {
		t.Fatal(err)
	}
	var line string
	for _, candidate := range strings.Split(log.String(), "\n") {
		if strings.Contains(candidate, "provider_sync.route_completed") {
			line = candidate
		}
	}
	for _, want := range []string{"deployments_synced=3", "big_synced=9007199254740993", "recovery=none", "effects_written=1"} {
		if !strings.Contains(line, want) {
			t.Fatalf("completion line lacks %q: %s", want, line)
		}
	}
	for _, absent := range []string{"provider-", "result=", "repo=", "detail", "note", "raw", "label_synced", "ratio_synced", "typed", "other_total"} {
		if strings.Contains(line, absent) {
			t.Fatalf("completion line carries %q: %s", absent, line)
		}
	}
}

// TestProtectedKeyClassifierTable pins the protected vocabulary and runs the
// classifier over every spelling a provider or a route can give a key.
func TestProtectedKeyClassifierTable(t *testing.T) {
	wantWords := []string{"apikey", "auth", "authorization", "bearer", "cert", "ciphertext", "cookie", "credential",
		"header", "passwd", "password", "pem", "secret", "session", "signature", "token"}
	gotWords := make([]string, 0, len(protectedKeyWords))
	for word := range protectedKeyWords {
		gotWords = append(gotWords, word)
	}
	sort.Strings(gotWords)
	if strings.Join(gotWords, ",") != strings.Join(wantWords, ",") {
		t.Fatalf("protected words=%v want %v: change the vocabulary here and in its pin together", gotWords, wantWords)
	}
	wantPairs := []string{"access_key", "api_key", "client_secret", "integration_config", "private_key",
		"raw_payload", "request_body", "response_body", "source_metadata"}
	gotPairs := make([]string, 0, len(protectedKeyWordPairs))
	for pair := range protectedKeyWordPairs {
		gotPairs = append(gotPairs, pair[0]+"_"+pair[1])
	}
	sort.Strings(gotPairs)
	if strings.Join(gotPairs, ",") != strings.Join(wantPairs, ",") {
		t.Fatalf("protected pairs=%v want %v", gotPairs, wantPairs)
	}
	for key, want := range map[string]string{
		"token": "token", "Token": "token", "TOKEN": "token", "tokens": "token", "access_token": "token",
		"accessToken": "token", "AccessToken": "token", "access-token": "token", "access.token": "token",
		"ACCESSTOKEN": "token", "apitoken": "token", "x-auth-token": "auth", "Authorization": "authorization",
		"authHeader": "auth", "api_key": "api_key", "apiKey": "api_key", "APIKey": "api_key", "x-api-key": "api_key",
		"apikey": "apikey", "APIKEY": "apikey", "private_key": "private_key", "privateKeyPem": "private_key",
		"clientSecret": "client_secret", "client_secret": "client_secret", "secrets": "secret", "userPassword": "password",
		"passwd": "passwd", "sourceMetadata": "source_metadata", "source-metadata": "source_metadata",
		"integration_config": "integration_config", "integrationConfig": "integration_config",
		"rawPayload": "raw_payload", "response_body": "response_body", "requestBody": "request_body",
		"headers": "header", "Set-Cookie": "cookie", "sessionId": "session", "signature": "signature",
		"tls.cert": "cert", "ciphertext": "ciphertext", "Bearer": "bearer", "credentials": "credential",
		"accessKeyId": "access_key", "raw_payloads": "raw_payload", "response_bodies": "response_body",
		"requestBodies": "request_body", "api_keys": "api_key", "privateKeys": "private_key",
		"access_keys": "access_key", "client_secrets": "client_secret", "source_metadatas": "source_metadata",
		"integration_configs": "integration_config",
	} {
		got, protected := isPreparedRouteSensitiveKeyName(key)
		if !protected || got != want {
			t.Errorf("key %q classified %q protected=%v want %q", key, got, protected, want)
		}
	}
	for _, key := range []string{"author", "author_name", "authored_at", "concert", "keyboard", "key", "body",
		"payload", "response", "metadata", "config", "name", "prs_synced", "temperature",
		"repo_id", "merged_at", "pull_request_number", "headline", "assessment", "keys", "bodies", "payloads",
		"access", "business", "status", "raw_bodies", "access_payloads"} {
		if name, protected := isPreparedRouteSensitiveKeyName(key); protected {
			t.Errorf("key %q classified protected as %q", key, name)
		}
	}
}
