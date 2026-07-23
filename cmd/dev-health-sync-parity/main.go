// dev-health-sync-parity compares the existing Python and Go read-only
// sync-dispatch observers at one PostgreSQL REPEATABLE READ snapshot. It is an
// offline proof tool: it has no worker loop, does not claim rows, and never
// writes to PostgreSQL or a queue.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/syncdispatchcontract"
	"github.com/full-chaos/dev-health-ops/internal/syncreconciler"
	"github.com/jackc/pgx/v5"
)

const (
	defaultContractRoot = "contracts/sync-dispatch/v1"
	defaultPython       = ".venv/bin/python"
	pythonHelper        = "scripts/worker/observe_sync_dispatch_parity.py"
	maxHelperOutput     = 32 * 1024
)

var digestPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

type commandConfig struct {
	DatabaseURI  string
	Python       string
	Cutoff       time.Time
	CutoffSource string
	Limit        int
}

type parityKind struct {
	Kind          string `json:"kind"`
	Route         string `json:"route"`
	DuePending    int64  `json:"due_pending"`
	ExpiredClaims int64  `json:"expired_claims"`
}

// parityObservation is the existing redacted Python observation schema. It
// intentionally has no identifier, tenant, payload, token, or URI field.
type parityObservation struct {
	Event             string       `json:"event"`
	Runtime           string       `json:"runtime"`
	ObservedAt        string       `json:"observed_at"`
	Limit             int          `json:"limit"`
	PredicateVersion  string       `json:"predicate_version"`
	DigestVersion     string       `json:"digest_version"`
	CandidateDigest   string       `json:"candidate_digest"`
	SampledCandidates int64        `json:"sampled_candidates"`
	Truncated         bool         `json:"truncated"`
	UnknownKindCount  int64        `json:"unknown_kind_count"`
	CeleryDuePending  int64        `json:"celery_due_pending"`
	RiverDuePending   int64        `json:"river_due_pending"`
	Kinds             []parityKind `json:"kinds"`
}

type mismatch struct {
	Field    string `json:"field"`
	Expected any    `json:"expected"`
	Actual   any    `json:"actual"`
}

type report struct {
	Status       string     `json:"status"`
	Cutoff       string     `json:"cutoff"`
	CutoffSource string     `json:"cutoff_source"`
	Limit        int        `json:"limit"`
	SnapshotMode string     `json:"snapshot_mode"`
	Mismatches   []mismatch `json:"mismatches,omitempty"`
	Reason       string     `json:"reason,omitempty"`
}

type pythonRunner func(context.Context, commandConfig, string) (parityObservation, error)

func main() {
	os.Exit(execute(context.Background(), os.Args[1:], os.LookupEnv, os.Stdout, os.Stderr, runPythonObserver))
}

func execute(
	parent context.Context,
	args []string,
	lookup func(string) (string, bool),
	stdout io.Writer,
	stderr io.Writer,
	runPython pythonRunner,
) int {
	if len(args) == 1 && (args[0] == "--help" || args[0] == "-h" || args[0] == "help") {
		printUsage(stdout)
		return 0
	}
	config, err := parseConfig(args, lookup, stderr)
	if err != nil {
		writeReport(stdout, report{Status: "error", Reason: "configuration_error"})
		return 2
	}
	if runPython == nil {
		writeReport(stdout, report{Status: "error", Reason: "configuration_error"})
		return 2
	}

	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	connection, err := pgx.Connect(ctx, config.DatabaseURI)
	if err != nil {
		writeReport(stdout, report{Status: "error", Reason: "database_unavailable"})
		return 1
	}
	defer connection.Close(ctx)

	tx, err := connection.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		writeReport(stdout, report{Status: "error", Reason: "database_unavailable"})
		return 1
	}
	// pg_export_snapshot is valid only while this transaction remains open.
	// Deliberately rollback even after successful, read-only observations.
	defer func() { _ = tx.Rollback(context.Background()) }()

	if config.CutoffSource == "database_clock" {
		if err := tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&config.Cutoff); err != nil {
			writeReport(stdout, report{Status: "error", Reason: "database_unavailable"})
			return 1
		}
		config.Cutoff = config.Cutoff.UTC()
	}
	if config.Cutoff.Nanosecond()%1_000 != 0 {
		writeReport(stdout, report{Status: "error", Reason: "cutoff_precision_unsupported"})
		return 2
	}

	var snapshotID string
	if err := tx.QueryRow(ctx, "SELECT pg_export_snapshot()").Scan(&snapshotID); err != nil {
		writeReport(stdout, report{Status: "error", Reason: "snapshot_unavailable"})
		return 1
	}

	registry, err := syncdispatchcontract.Load(defaultContractRoot)
	if err != nil {
		writeReport(stdout, report{Status: "error", Reason: "route_contract_unavailable"})
		return 1
	}
	python, err := runPython(ctx, config, snapshotID)
	if err != nil || !validPythonObservation(python, config) {
		writeReport(stdout, report{Status: "error", Reason: "python_observation_unavailable"})
		return 1
	}
	goObservation, err := syncreconciler.ObserveSnapshot(ctx, tx, registry, config.Cutoff, config.Limit)
	if err != nil {
		writeReport(stdout, report{Status: "error", Reason: "go_observation_unavailable"})
		return 1
	}
	goParity := fromGoObservation(goObservation)
	mismatches := compareObservations(python, goParity)
	result := report{
		Status:       "match",
		Cutoff:       canonicalCutoff(config.Cutoff),
		CutoffSource: config.CutoffSource,
		Limit:        config.Limit,
		SnapshotMode: "repeatable_read_exported",
	}
	if len(mismatches) > 0 {
		result.Status = "mismatch"
		result.Mismatches = mismatches
		writeReport(stdout, result)
		return 1
	}
	writeReport(stdout, result)
	return 0
}

func parseConfig(args []string, lookup func(string) (string, bool), stderr io.Writer) (commandConfig, error) {
	flags := flag.NewFlagSet("dev-health-sync-parity", flag.ContinueOnError)
	flags.SetOutput(stderr)
	python := flags.String("python", defaultPython, "Python executable for the redacted observer helper")
	cutoff := flags.String("cutoff", "", "UTC RFC3339 cutoff; defaults to the exporter database clock")
	limit := flags.Int("limit", 0, "required bounded candidate limit (1-100)")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 || *limit < 1 || *limit > 100 || *python == "" {
		return commandConfig{}, errors.New("invalid configuration")
	}
	databaseURI, ok := lookup("POSTGRES_URI")
	if !ok || databaseURI == "" {
		databaseURI, ok = lookup("DATABASE_URI")
	}
	if !ok || databaseURI == "" {
		return commandConfig{}, errors.New("database URI is required")
	}
	databaseURI = normalizePostgresURI(databaseURI)
	config := commandConfig{
		DatabaseURI:  databaseURI,
		Python:       *python,
		Limit:        *limit,
		CutoffSource: "database_clock",
	}
	if *cutoff == "" {
		return config, nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, *cutoff)
	if err != nil || parsed.Nanosecond()%1_000 != 0 {
		return commandConfig{}, errors.New("invalid cutoff")
	}
	config.Cutoff = parsed.UTC()
	config.CutoffSource = "argument"
	return config, nil
}

func printUsage(output io.Writer) {
	flags := flag.NewFlagSet("dev-health-sync-parity", flag.ContinueOnError)
	flags.SetOutput(output)
	flags.String("python", defaultPython, "Python executable for the redacted observer helper")
	flags.String("cutoff", "", "UTC RFC3339 cutoff; defaults to the exporter database clock")
	flags.Int("limit", 0, "required bounded candidate limit (1-100)")
	fmt.Fprintln(output, "Usage: dev-health-sync-parity --limit 1..100 [--cutoff RFC3339 UTC]")
	flags.PrintDefaults()
}

func runPythonObserver(ctx context.Context, config commandConfig, snapshotID string) (parityObservation, error) {
	command := exec.CommandContext(
		ctx,
		config.Python,
		pythonHelper,
		"--cutoff", canonicalCutoff(config.Cutoff),
		"--limit", fmt.Sprintf("%d", config.Limit),
	)
	command.Env = replacedEnvironment(os.Environ(), map[string]string{
		"SYNC_DISPATCH_PARITY_DATABASE_URI": config.DatabaseURI,
		"SYNC_DISPATCH_PARITY_SNAPSHOT_ID":  snapshotID,
	})
	var output boundedBuffer
	output.limit = maxHelperOutput
	command.Stdout = &output
	command.Stderr = io.Discard
	if err := command.Run(); err != nil || output.overflow {
		return parityObservation{}, errors.New("python helper unavailable")
	}
	decoder := json.NewDecoder(bytes.NewReader(output.Bytes()))
	decoder.DisallowUnknownFields()
	var observation parityObservation
	if err := decoder.Decode(&observation); err != nil {
		return parityObservation{}, errors.New("python observation invalid")
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return parityObservation{}, errors.New("python observation has trailing data")
	}
	return observation, nil
}

type boundedBuffer struct {
	bytes.Buffer
	limit    int
	overflow bool
}

func (buffer *boundedBuffer) Write(value []byte) (int, error) {
	if buffer.Len()+len(value) > buffer.limit {
		remaining := buffer.limit - buffer.Len()
		if remaining > 0 {
			_, _ = buffer.Buffer.Write(value[:remaining])
		}
		buffer.overflow = true
		return len(value), nil
	}
	return buffer.Buffer.Write(value)
}

func replacedEnvironment(base []string, replacements map[string]string) []string {
	result := make([]string, 0, len(base)+len(replacements))
	for _, item := range base {
		name, _, found := strings.Cut(item, "=")
		if !found {
			continue
		}
		if _, replaced := replacements[name]; !replaced {
			result = append(result, item)
		}
	}
	for name, value := range replacements {
		result = append(result, name+"="+value)
	}
	return result
}

// normalizePostgresURI accepts the same documented SQLAlchemy driver aliases
// as the worker runtime. pgx and this helper's synchronous SQLAlchemy reader
// both need the unqualified PostgreSQL URI form, while credentials and query
// parameters remain byte-for-byte untouched.
func normalizePostgresURI(uri string) string {
	for _, driverScheme := range []string{
		"postgresql+asyncpg://",
		"postgresql+psycopg://",
		"postgresql+psycopg2://",
		"postgres+asyncpg://",
	} {
		if strings.HasPrefix(strings.ToLower(uri), driverScheme) {
			return "postgresql://" + uri[len(driverScheme):]
		}
	}
	return uri
}

func fromGoObservation(observation syncreconciler.Observation) parityObservation {
	kinds := make([]parityKind, 0, len(observation.Kinds))
	for _, kind := range observation.Kinds {
		kinds = append(kinds, parityKind{
			Kind:          kind.Kind,
			Route:         kind.Route,
			DuePending:    kind.DuePending,
			ExpiredClaims: kind.ExpiredClaims,
		})
	}
	return parityObservation{
		Event:             "sync_dispatch_parity_observation",
		Runtime:           "go_observer",
		ObservedAt:        canonicalCutoff(observation.ObservedAt),
		Limit:             observation.Limit,
		PredicateVersion:  observation.PredicateVersion,
		DigestVersion:     observation.DigestVersion,
		CandidateDigest:   observation.CandidateDigest,
		SampledCandidates: observation.SampledCandidates,
		Truncated:         observation.Truncated,
		UnknownKindCount:  observation.UnknownKindCount,
		CeleryDuePending:  observation.CeleryDuePending,
		RiverDuePending:   observation.RiverDuePending,
		Kinds:             kinds,
	}
}

func validPythonObservation(observation parityObservation, config commandConfig) bool {
	if observation.Event != "sync_dispatch_parity_observation" || observation.Runtime != "celery" ||
		observation.ObservedAt != canonicalCutoff(config.Cutoff) || observation.Limit != config.Limit ||
		observation.PredicateVersion != syncreconciler.PredicateVersion ||
		observation.DigestVersion != syncreconciler.DigestVersion ||
		!digestPattern.MatchString(observation.CandidateDigest) ||
		observation.SampledCandidates < 0 || observation.SampledCandidates > int64(config.Limit) ||
		observation.UnknownKindCount < 0 || observation.UnknownKindCount > observation.SampledCandidates ||
		(observation.Truncated && observation.SampledCandidates != int64(config.Limit)) ||
		len(observation.Kinds) != 4 {
		return false
	}
	wantKinds := []string{
		syncdispatchcontract.KindDispatchSyncRun,
		syncdispatchcontract.KindFinalizeSyncRun,
		syncdispatchcontract.KindPostSync,
		syncdispatchcontract.KindReferenceDiscovery,
	}
	var known, celery, river int64
	for index, kind := range observation.Kinds {
		if kind.Kind != wantKinds[index] || (kind.Route != syncdispatchcontract.RouteCelery && kind.Route != syncdispatchcontract.RouteRiver) ||
			kind.DuePending < 0 || kind.ExpiredClaims < 0 || kind.ExpiredClaims > kind.DuePending {
			return false
		}
		known += kind.DuePending
		if kind.Route == syncdispatchcontract.RouteCelery {
			celery += kind.DuePending
		} else {
			river += kind.DuePending
		}
	}
	return known+observation.UnknownKindCount == observation.SampledCandidates &&
		celery == observation.CeleryDuePending && river == observation.RiverDuePending
}

func compareObservations(expected, actual parityObservation) []mismatch {
	comparisons := []struct {
		field string
		left  any
		right any
	}{
		{"event", expected.Event, actual.Event},
		{"observed_at", expected.ObservedAt, actual.ObservedAt},
		{"limit", expected.Limit, actual.Limit},
		{"predicate_version", expected.PredicateVersion, actual.PredicateVersion},
		{"digest_version", expected.DigestVersion, actual.DigestVersion},
		{"candidate_digest", expected.CandidateDigest, actual.CandidateDigest},
		{"sampled_candidates", expected.SampledCandidates, actual.SampledCandidates},
		{"truncated", expected.Truncated, actual.Truncated},
		{"unknown_kind_count", expected.UnknownKindCount, actual.UnknownKindCount},
		{"celery_due_pending", expected.CeleryDuePending, actual.CeleryDuePending},
		{"river_due_pending", expected.RiverDuePending, actual.RiverDuePending},
	}
	result := make([]mismatch, 0)
	for _, comparison := range comparisons {
		if comparison.left != comparison.right {
			result = append(result, mismatch{Field: comparison.field, Expected: comparison.left, Actual: comparison.right})
		}
	}
	if len(expected.Kinds) != len(actual.Kinds) {
		return append(result, mismatch{Field: "kinds.length", Expected: len(expected.Kinds), Actual: len(actual.Kinds)})
	}
	for index := range expected.Kinds {
		left, right := expected.Kinds[index], actual.Kinds[index]
		for _, comparison := range []struct {
			field string
			left  any
			right any
		}{
			{"kind", left.Kind, right.Kind},
			{"route", left.Route, right.Route},
			{"due_pending", left.DuePending, right.DuePending},
			{"expired_claims", left.ExpiredClaims, right.ExpiredClaims},
		} {
			if comparison.left != comparison.right {
				result = append(result, mismatch{
					Field:    "kinds." + left.Kind + "." + comparison.field,
					Expected: comparison.left,
					Actual:   comparison.right,
				})
			}
		}
	}
	return result
}

func canonicalCutoff(value time.Time) string {
	return value.UTC().Format("2006-01-02T15:04:05.000000000Z")
}

func writeReport(output io.Writer, value report) {
	encoder := json.NewEncoder(output)
	encoder.SetEscapeHTML(true)
	_ = encoder.Encode(value)
}
