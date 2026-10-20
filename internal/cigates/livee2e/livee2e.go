// Package livee2e holds the checks and fixture steps the Live Backend E2E harness runs between its Go binaries:
// JSON assertions on the health, meta and home answers, the registered edge request, the proof service principal
// fixture, the e2e access token, the Valkey readiness ping. They replace the Python programs the harness used to
// embed, so the workflow installs no Python.
package livee2e

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

type doc = map[string]any

func decode(payload []byte) (doc, error) {
	var out doc
	if err := json.Unmarshal(payload, &out); err != nil {
		return nil, fmt.Errorf("not a JSON object: %w", err)
	}
	if out == nil {
		return nil, errors.New("not a JSON object")
	}
	return out, nil
}

func obj(v any) doc {
	m, _ := v.(doc)
	if m == nil {
		return doc{}
	}
	return m
}

// truthy mirrors the truthiness the checks were written with: nil, false, zero, "" and empty containers are false.
func truthy(v any) bool {
	switch x := v.(type) {
	case nil:
		return false
	case bool:
		return x
	case float64:
		return x != 0
	case string:
		return x != ""
	case []any:
		return len(x) > 0
	case doc:
		return len(x) > 0
	}
	return true
}

// AssertHealth checks a /health answer: status ok, ClickHouse ok, Postgres ok or not configured.
func AssertHealth(payload []byte) error {
	p, err := decode(payload)
	if err != nil {
		return err
	}
	if p["status"] != "ok" {
		return fmt.Errorf("health status is not ok: %s", payload)
	}
	services := obj(p["services"])
	if services["clickhouse"] != "ok" {
		return fmt.Errorf("clickhouse is not ok: %v", services)
	}
	if pg := services["postgres"]; pg != "ok" && pg != "not_configured" {
		return fmt.Errorf("postgres is not ok or not_configured: %v", services)
	}
	return nil
}

// AssertMeta checks a /api/v1/meta answer.
func AssertMeta(payload []byte) error {
	p, err := decode(payload)
	if err != nil {
		return err
	}
	if p["backend"] != "clickhouse" {
		return fmt.Errorf("backend is not clickhouse: %s", payload)
	}
	limits := obj(p["limits"])
	if limits["max_days"] != float64(365) {
		return fmt.Errorf("limits.max_days is not 365: %v", limits)
	}
	if limits["max_repos"] != float64(1000) {
		return fmt.Errorf("limits.max_repos is not 1000: %v", limits)
	}
	supported, _ := p["supported_endpoints"].([]any)
	for _, e := range supported {
		if e == "/api/v1/home" {
			return nil
		}
	}
	return fmt.Errorf("/api/v1/home is not a supported endpoint: %v", supported)
}

var homeAllowedStates = map[string]bool{"ok": true, "degraded": true, "down": true, "stale": true, "unknown": true, "not_configured": true, "error": true}

// AssertHome checks a /api/v1/home answer for the seeded fixture provider.
func AssertHome(payload []byte, fixtureProvider string) error {
	p, err := decode(payload)
	if err != nil {
		return err
	}
	freshness := obj(p["freshness"])
	if _, ok := freshness["last_ingested_at"]; !ok {
		return fmt.Errorf("freshness has no last_ingested_at: %v", freshness)
	}
	sources := obj(freshness["sources"])
	if fixtureProvider == "" {
		fixtureProvider = "github"
	}
	expected := map[string]bool{"ci": true}
	if fixtureProvider != "synthetic" {
		expected[fixtureProvider] = true
	}
	got := map[string]bool{}
	for k := range sources {
		got[k] = true
	}
	if !reflect.DeepEqual(got, expected) {
		return fmt.Errorf("freshness sources are %v, want %v", sortedKeys(got), sortedKeys(expected))
	}
	for key := range expected {
		state, _ := sources[key].(string)
		if !homeAllowedStates[strings.ToLower(state)] {
			return fmt.Errorf("source %q has state %v: %v", key, sources[key], sources)
		}
	}
	coverage := obj(freshness["coverage"])
	pct, err := number(coverage["repos_covered_pct"])
	if err != nil || pct <= 0 {
		return fmt.Errorf("coverage.repos_covered_pct is not above zero: %v", coverage)
	}
	deltas, _ := p["deltas"].([]any)
	if len(deltas) < 1 {
		return errors.New("deltas is empty")
	}
	hasMove := false
	for _, raw := range deltas {
		row := obj(raw)
		for _, key := range []string{"metric", "value", "has_data", "has_prior_data", "rate_state"} {
			if _, ok := row[key]; !ok {
				return fmt.Errorf("a delta has no %s: %v", key, row)
			}
		}
		if truthy(row["has_data"]) && truthy(row["has_prior_data"]) {
			hasMove = true
		}
	}
	tiles := obj(p["tiles"])
	for _, key := range []string{"understand", "measure", "align", "execute"} {
		if _, ok := tiles[key]; !ok {
			return fmt.Errorf("tiles has no %s: %v", key, tiles)
		}
	}
	constraint := obj(p["constraint"])
	if hasMove {
		if !truthy(constraint["title"]) || !truthy(constraint["evidence"]) {
			return fmt.Errorf("a metric moved between two windows but the constraint card is empty: %v", constraint)
		}
		return nil
	}
	if truthy(constraint["title"]) || truthy(constraint["claim"]) || truthy(constraint["evidence"]) {
		return fmt.Errorf("no metric has data in both windows but the constraint card makes a claim: %v", constraint)
	}
	return nil
}

func number(v any) (float64, error) {
	switch x := v.(type) {
	case nil:
		return 0, nil
	case float64:
		return x, nil
	case string:
		return strconv.ParseFloat(x, 64)
	}
	return 0, fmt.Errorf("not a number: %v", v)
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

type featureFlagsVariables struct {
	OrgID           string  `json:"orgId"`
	Provider        *string `json:"provider"`
	Project         *string `json:"project"`
	IncludeArchived bool    `json:"includeArchived"`
	Limit           int     `json:"limit"`
}

// EdgeRequest builds the body the edge checks post: the registered document of the operation from documents.json,
// with the variables its proof request carries (internal/goapiproof/operations.go).
func EdgeRequest(documents []byte, operation, org string) ([]byte, error) {
	var entries []struct {
		Operation string `json:"operation"`
		Document  string `json:"document"`
	}
	if err := json.Unmarshal(documents, &entries); err != nil {
		return nil, fmt.Errorf("documents.json: %w", err)
	}
	var texts []string
	for _, e := range entries {
		if e.Operation == operation {
			texts = append(texts, e.Document)
		}
	}
	if len(texts) != 1 {
		return nil, fmt.Errorf("documents.json does not carry exactly one document for %s", operation)
	}
	var vars any
	switch operation {
	case "featureFlags":
		vars = featureFlagsVariables{OrgID: org, IncludeArchived: false, Limit: 100}
	default:
		return nil, fmt.Errorf("no request variables for %s", operation)
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	body := struct {
		Query     string `json:"query"`
		Variables any    `json:"variables"`
	}{texts[0], vars}
	if err := enc.Encode(body); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

// JWTSecret is the signing key the e2e token and the api share: the hex sha256 of the settings encryption key.
func JWTSecret(encKey string) string {
	sum := sha256.Sum256([]byte(encKey))
	return hex.EncodeToString(sum[:])
}

// E2E identities the access token names.
const (
	E2EUserID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	e2eEmail  = "e2e@test.local"
	// DefaultOrgID is the organization the harness seeds and measures.
	DefaultOrgID = "11111111-2222-4333-8444-555555555555"
	// DefaultEncryptionKey is the settings encryption key the harness runs with when none is set.
	DefaultEncryptionKey = "dev-key-not-for-prod"
)

// MintToken signs the e2e admin access token (HS256, one hour).
func MintToken(secret, orgID string, now time.Time, jti string) (string, error) {
	claims := jwt.MapClaims{
		"sub":          E2EUserID,
		"email":        e2eEmail,
		"org_id":       orgID,
		"role":         "admin",
		"is_superuser": false,
		"type":         "access",
		"exp":          now.Add(time.Hour).Unix(),
		"iat":          now.Unix(),
		"jti":          jti,
	}
	return jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(secret))
}

// ValkeyPing answers whether the Valkey at the redis:// URL replies PONG.
func ValkeyPing(ctx context.Context, redisURL string) error {
	u, err := url.Parse(redisURL)
	if err != nil || u.Host == "" {
		return fmt.Errorf("not a redis URL: %q", redisURL)
	}
	host := u.Host
	if u.Port() == "" {
		host = net.JoinHostPort(u.Hostname(), "6379")
	}
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", host)
	if err != nil {
		return err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	r := bufio.NewReader(conn)
	send := func(args ...string) (string, error) {
		var b strings.Builder
		fmt.Fprintf(&b, "*%d\r\n", len(args))
		for _, a := range args {
			fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(a), a)
		}
		if _, err := conn.Write([]byte(b.String())); err != nil {
			return "", err
		}
		line, err := r.ReadString('\n')
		return strings.TrimRight(line, "\r\n"), err
	}
	if pw, ok := u.User.Password(); ok {
		args := []string{"AUTH"}
		if user := u.User.Username(); user != "" {
			args = append(args, user)
		}
		if reply, err := send(append(args, pw)...); err != nil || !strings.HasPrefix(reply, "+OK") {
			return fmt.Errorf("AUTH refused: %q %v", reply, err)
		}
	}
	reply, err := send("PING")
	if err != nil {
		return err
	}
	if reply != "+PONG" {
		return fmt.Errorf("PING answered %q", reply)
	}
	return nil
}
