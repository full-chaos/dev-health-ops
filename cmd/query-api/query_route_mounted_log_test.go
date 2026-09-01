package main

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/principal"
)

// ---------------------------------------------------------------------------
// mountedRouteLogMessage / sortedOperationNames: pure-function contract.
// These exercise the actual functions newQueryHandler calls (query_route.go),
// not a reimplementation, against representative maps -- proving the
// mechanism itself never drops, duplicates, or invents an operation name
// regardless of what map it is handed.
// ---------------------------------------------------------------------------

func TestSortedOperationNames_ReturnsExactlyTheKeysSorted(t *testing.T) {
	m := map[string]string{
		"workGraphEdges": "digest-c",
		"cognitiveLoad":  "digest-a",
		"hotspots":       "digest-b",
	}
	got := sortedOperationNames(m)
	want := []string{"cognitiveLoad", "hotspots", "workGraphEdges"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v (sorted order)", got, want)
		}
	}
}

func TestMountedRouteLogMessage_NamesExactlyTheGivenMapsKeys(t *testing.T) {
	// A representative map the SAME shape as production's digestByOperation
	// (operation name -> digest), but deliberately NOT copied from it --
	// this proves mountedRouteLogMessage's mechanism (it lists exactly its
	// input's keys, once each) independent of what the real map currently
	// contains, so this test cannot itself go stale the way the old
	// hand-typed main.go literal did.
	m := map[string]string{
		"alpha": "digest-1",
		"beta":  "digest-2",
		"gamma": "digest-3",
	}
	msg := mountedRouteLogMessage(m)

	if !strings.HasPrefix(msg, "query-api: /query route mounted (") || !strings.HasSuffix(msg, ")") {
		t.Fatalf("mounted route log message %q does not match the expected shape", msg)
	}
	open := strings.Index(msg, "(")
	loggedNames := strings.Split(msg[open+1:len(msg)-1], ", ")

	logged := make(map[string]bool, len(loggedNames))
	for _, name := range loggedNames {
		logged[name] = true
	}
	if len(logged) != len(loggedNames) {
		t.Fatalf("mounted route log message %q names a duplicate operation: %v", msg, loggedNames)
	}
	if len(logged) != len(m) {
		t.Fatalf("mounted route log message names %d operations, want %d (msg=%q)", len(logged), len(m), msg)
	}
	for operation := range m {
		if !logged[operation] {
			t.Fatalf("input map has operation %q but the mounted-route log message %q does not name it", operation, msg)
		}
	}
}

func TestMountedRouteLogMessage_EmptyMapNamesNoOperations(t *testing.T) {
	msg := mountedRouteLogMessage(map[string]string{})
	if msg != "query-api: /query route mounted ()" {
		t.Fatalf("got %q, want the empty-list shape", msg)
	}
}

// ---------------------------------------------------------------------------
// End-to-end: newQueryHandler's ACTUAL printed log line, captured from a
// real call (fake ClickHouse client + a lazily-dialed, never-contacted
// Postgres pool -- pgxpool.New does not dial eagerly, see CHAOS-4512's
// query_route_readyz_integration_test.go for the same premise proven
// against buildQueryRoute), cross-checked against
// src/dev_health_ops/api/graphql/go_api_operations.json -- the CHECKED-IN
// catalog test_go_api_operation_catalog.py's
// test_checked_in_catalog_has_not_drifted_from_registrydump already keeps
// byte-for-byte in sync with cmd/query-api/tools/registrydump's independent
// static parse of THIS package's digestByOperation literal. Comparing
// against that file (rather than a second hand-typed operation list here)
// means this test cannot itself become the next stale list: if a document
// is ever added to digestByOperation without regenerating the catalog, the
// Python-side drift test fails first and this one fails for the same root
// cause, never silently agreeing with a copy that also went stale.
// ---------------------------------------------------------------------------

type nopQueryClient struct{}

func (nopQueryClient) Query(ctx context.Context, statement string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	return nil, errors.New("nopQueryClient: construction-only fixture, never queried by this test")
}

func writeMountedLogTestJWKS(t *testing.T) string {
	t.Helper()
	_, pub, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate ed25519 key: %v", err)
	}
	doc := map[string]any{
		"keys": []map[string]any{
			{
				"kty": "OKP",
				"crv": "Ed25519",
				"x":   base64.RawURLEncoding.EncodeToString(pub),
				"kid": "test-key",
				"use": "sig",
				"alg": "EdDSA",
			},
		},
	}
	encoded, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal jwks: %v", err)
	}
	path := filepath.Join(t.TempDir(), "jwks.json")
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatalf("write jwks: %v", err)
	}
	return path
}

func TestNewQueryHandler_LoggedOperationSetMatchesCheckedInCatalog(t *testing.T) {
	pgPool, err := pgxpool.New(context.Background(), "postgres://nobody:nobody@127.0.0.1:1/nonexistent")
	if err != nil {
		t.Fatalf("pgxpool.New: unexpected error (pgxpool.New must not dial eagerly): %v", err)
	}
	defer pgPool.Close()

	verifier, err := principal.NewVerifier(writeMountedLogTestJWKS(t), "dev-health-ops-edge", "query-api")
	if err != nil {
		t.Fatalf("principal.NewVerifier: %v", err)
	}

	var logBuf strings.Builder
	prevOutput := log.Writer()
	prevFlags := log.Flags()
	log.SetOutput(&logBuf)
	log.SetFlags(0)
	defer func() {
		log.SetOutput(prevOutput)
		log.SetFlags(prevFlags)
	}()

	_ = newQueryHandler(nopQueryClient{}, pgPool, verifier, "sha256:test-schema-digest")

	logged := logBuf.String()
	const marker = "query-api: /query route mounted ("
	start := strings.Index(logged, marker)
	if start < 0 {
		t.Fatalf("newQueryHandler did not print the mount-confirmation log line; captured output: %q", logged)
	}
	rest := logged[start+len(marker):]
	end := strings.Index(rest, ")")
	if end < 0 {
		t.Fatalf("mount-confirmation log line has no closing paren; captured output: %q", logged)
	}
	loggedOperations := make(map[string]bool)
	for _, name := range strings.Split(rest[:end], ", ") {
		loggedOperations[name] = true
	}

	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve repo root: %v", err)
	}
	catalogPath := filepath.Join(repoRoot, "src", "dev_health_ops", "api", "graphql", "go_api_operations.json")
	catalogBytes, err := os.ReadFile(catalogPath)
	if err != nil {
		t.Fatalf("read checked-in catalog %s: %v", catalogPath, err)
	}
	var catalog []struct {
		Operation string `json:"operation"`
	}
	if err := json.Unmarshal(catalogBytes, &catalog); err != nil {
		t.Fatalf("parse checked-in catalog: %v", err)
	}
	if len(catalog) == 0 {
		t.Fatalf("checked-in catalog %s is empty -- cannot cross-check against it", catalogPath)
	}

	if len(loggedOperations) != len(catalog) {
		t.Fatalf("logged %d operations, checked-in catalog has %d -- logged=%v",
			len(loggedOperations), len(catalog), loggedOperations)
	}
	for _, entry := range catalog {
		if !loggedOperations[entry.Operation] {
			t.Fatalf("checked-in catalog registers %q but newQueryHandler's mount log line %q does not name it",
				entry.Operation, logged)
		}
	}
}
