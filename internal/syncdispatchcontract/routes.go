// Package syncdispatchcontract validates the frozen v1 sync-dispatch routes.
//
// It deliberately has no runtime integration. Consumers may load and look up
// the checked-in policy, but route execution remains outside this package.
package syncdispatchcontract

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
	"unicode/utf8"
)

const (
	// Filename is the only artifact accepted by Load.
	Filename = "transport-routes.json"

	maxArtifactBytes = 16 * 1024
	maxJSONDepth     = 8

	KindDispatchSyncRun    = "dispatch_sync_run"
	KindFinalizeSyncRun    = "finalize_sync_run"
	KindPostSync           = "post_sync"
	KindReferenceDiscovery = "reference_discovery"

	// RiverQueue is the ONE River queue every river-routed sync-dispatch kind
	// is published into. It is declared here, beside the frozen kinds, because
	// the queue name is the join between this route plane and the bounded jobs
	// registry plane: both put rows in river.river_job, and a reader of that
	// table (the startup contract-version check, for one) can only resolve a
	// kind if it knows which planes may occupy the queue it is reading.
	//
	// It used to be a bare "sync" literal in the reconciler and a private
	// const in the worker, with nothing tying either to this package. Nothing
	// then made it visible that queue "sync" carries dispatch_sync_run as well
	// as the registry's sync.team_autoimport, and the worker refused to start
	// whenever those rows were pending (CHAOS-3938).
	RiverQueue = "sync"

	DeliveryAtLeastOnce = "at_least_once"

	RouteCelery = "celery"
	RouteRiver  = "river"
)

// dispatchStaleSecondsEnv and defaultDispatchStaleSeconds are the same
// env var and default the Python side reads: sync/guard.py's
// _stale_dispatch_seconds_guard, sync/budget_guard.py._stale_dispatch_cutoff,
// and workers/sync_units.py._stale_dispatch_seconds all resolve
// SYNC_UNIT_DISPATCH_STALE_SECONDS with a 900-second (15-minute) fallback; the
// checked-in deploy manifests (docker-compose, helm, kubernetes) all pin it
// to that same 900.
const (
	dispatchStaleSecondsEnv     = "SYNC_UNIT_DISPATCH_STALE_SECONDS"
	defaultDispatchStaleSeconds = 900
)

// DispatchStaleAge is how long a DISPATCHING sync_run_units row stays
// evidence of live work before it is treated as orphaned instead. A row
// younger than this may still be an unclaimed Celery message; a row older
// than this with no claim never will be.
//
// It reads SYNC_UNIT_DISPATCH_STALE_SECONDS at call time -- the same
// operator-facing knob sync/guard.py's _stale_dispatch_seconds_guard and
// workers/sync_units.py's _stale_dispatch_seconds already read -- so a
// production override actually reaches both language planes instead of only
// one. Parse failures and an unset var fall back to the checked-in default;
// a parsed value clamps to a 1-second floor, mirroring those two functions'
// identical `max(1, int(getenv(..., "900")))` exactly -- NOT the codebase's
// separate max(0, ...) `_env_int` helper used for other settings. Zero and
// negative both being accepted was a real defect (CHAOS-3929 review round
// 2): a zero-second window makes every DISPATCHING row look orphaned
// immediately, and MutationPipelineConfig.valid() requires StaleDispatchAge
// > 0, so zero could also fail the reconciler's own config validation.
//
// Declared once here, the same fix as RiverQueue above (CHAOS-3938): before
// CHAOS-3929 this was a private 15-minute literal in the reconciler's mutation
// pipeline (internal/syncreconciler) with nothing tying it to the quiescer
// that also needs to know whether a DISPATCHING row is still live
// (internal/jobroute's PostgresCelerySyncProviderQuiescer), and neither read
// the env var Python operators actually tune. Two copies of the same number
// drift, and a Go-only-hardcoded copy silently ignores an operator override;
// both now go through this one function.
func DispatchStaleAge() time.Duration {
	seconds := defaultDispatchStaleSeconds
	if raw := os.Getenv(dispatchStaleSecondsEnv); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil {
			seconds = parsed
		}
	}
	if seconds < 1 {
		seconds = 1
	}
	return time.Duration(seconds) * time.Second
}

// Descriptor is a single immutable-by-value sync-dispatch route descriptor.
// No reference values are exposed from Registry.
type Descriptor struct {
	Kind          string `json:"kind"`
	Delivery      string `json:"delivery"`
	Route         string `json:"route"`
	RollbackRoute string `json:"rollback_route"`
}

type artifact struct {
	SchemaVersion int          `json:"schema_version"`
	Routes        []Descriptor `json:"routes"`
}

// Registry is immutable after Load and safe for concurrent lookups.
type Registry struct {
	byKind map[string]Descriptor
}

// Load reads the one bounded sync-dispatch route artifact rooted at root. It
// performs best-effort regular-file and symbolic-link checks, but callers must
// not treat those path checks as a TOCTOU guarantee for concurrently mutable
// filesystem paths.
func Load(root string) (*Registry, error) {
	data, err := readArtifact(root)
	if err != nil {
		return nil, err
	}

	var parsed artifact
	if err := decodeStrict(data, &parsed); err != nil {
		return nil, fmt.Errorf("decode %s: %w", Filename, err)
	}
	if err := parsed.validate(); err != nil {
		return nil, err
	}

	byKind := make(map[string]Descriptor, len(parsed.Routes))
	for _, descriptor := range parsed.Routes {
		byKind[descriptor.Kind] = descriptor
	}
	return &Registry{byKind: byKind}, nil
}

// Lookup returns a value copy of the descriptor for kind. Altering the
// returned value cannot change the registry's loaded policy.
func (registry *Registry) Lookup(kind string) (Descriptor, bool) {
	if registry == nil {
		return Descriptor{}, false
	}
	descriptor, ok := registry.byKind[kind]
	return descriptor, ok
}

func (parsed artifact) validate() error {
	if parsed.SchemaVersion != 1 {
		return errors.New("unsupported sync-dispatch route schema_version")
	}
	if len(parsed.Routes) != len(frozenDeliveries) {
		return errors.New("sync-dispatch routes must cover every frozen kind exactly once")
	}

	previous := ""
	seen := make(map[string]struct{}, len(parsed.Routes))
	for _, descriptor := range parsed.Routes {
		if descriptor.Kind <= previous {
			return errors.New("sync-dispatch routes must be lexicographically sorted by kind")
		}
		previous = descriptor.Kind
		if _, duplicate := seen[descriptor.Kind]; duplicate {
			return fmt.Errorf("duplicate sync-dispatch route kind %q", descriptor.Kind)
		}
		seen[descriptor.Kind] = struct{}{}

		expectedDelivery, ok := frozenDeliveries[descriptor.Kind]
		if !ok {
			return fmt.Errorf("sync-dispatch route kind %q is not frozen", descriptor.Kind)
		}
		if descriptor.Delivery != expectedDelivery {
			return fmt.Errorf("sync-dispatch route %s has invalid delivery", descriptor.Kind)
		}
		if !validRoutePair(descriptor.Route, descriptor.RollbackRoute) {
			return fmt.Errorf("sync-dispatch route %s has invalid transport pair", descriptor.Kind)
		}
	}
	return nil
}

// Kinds returns every frozen sync-dispatch kind, sorted. It is DERIVED from
// the frozen delivery table rather than restated, so a fifth kind added there
// is automatically visible to every consumer that has to enumerate this
// plane -- including the readiness check that must resolve each kind's rows in
// RiverQueue.
func Kinds() []string {
	kinds := make([]string, 0, len(frozenDeliveries))
	for kind := range frozenDeliveries {
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)
	return kinds
}

var frozenDeliveries = map[string]string{
	KindDispatchSyncRun:    DeliveryAtLeastOnce,
	KindFinalizeSyncRun:    DeliveryAtLeastOnce,
	KindPostSync:           DeliveryAtLeastOnce,
	KindReferenceDiscovery: DeliveryAtLeastOnce,
}

func validRoutePair(route, rollbackRoute string) bool {
	return rollbackRoute == RouteCelery && (route == RouteCelery || route == RouteRiver)
}

func readArtifact(root string) ([]byte, error) {
	if root == "" {
		return nil, errors.New("sync-dispatch contract root is required")
	}
	rootPath, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve sync-dispatch contract root: %w", err)
	}
	rootInfo, err := os.Lstat(rootPath)
	if err != nil {
		return nil, fmt.Errorf("inspect sync-dispatch contract root: %w", err)
	}
	if rootInfo.Mode()&os.ModeSymlink != 0 || !rootInfo.IsDir() {
		return nil, errors.New("sync-dispatch contract root must be a directory, not a symbolic link")
	}

	path := filepath.Join(rootPath, Filename)
	info, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("inspect %s: %w", Filename, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%s must be a regular file", Filename)
	}
	if info.Size() > maxArtifactBytes {
		return nil, fmt.Errorf("%s exceeds %d bytes", Filename, maxArtifactBytes)
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", Filename, err)
	}
	defer file.Close()
	openedInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat %s: %w", Filename, err)
	}
	if !openedInfo.Mode().IsRegular() {
		return nil, fmt.Errorf("%s must be a regular file", Filename)
	}
	data, err := io.ReadAll(io.LimitReader(file, maxArtifactBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", Filename, err)
	}
	if len(data) > maxArtifactBytes {
		return nil, fmt.Errorf("%s exceeds %d bytes", Filename, maxArtifactBytes)
	}
	return data, nil
}

func decodeStrict(data []byte, destination any) error {
	if len(data) == 0 {
		return errors.New("JSON value is empty")
	}
	if !utf8.Valid(data) {
		return errors.New("JSON must be UTF-8")
	}
	if err := validateJSONTokens(data); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return errors.New("JSON does not match sync-dispatch contract")
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("JSON has trailing data")
	}
	return nil
}

func validateJSONTokens(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := consumeJSONValue(decoder, 0); err != nil {
		return errors.New("invalid JSON structure")
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return errors.New("JSON has trailing data")
	}
	return nil
}

func consumeJSONValue(decoder *json.Decoder, depth int) error {
	if depth > maxJSONDepth {
		return errors.New("JSON nesting exceeds limit")
	}
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delimiter {
	case '{':
		keys := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return errors.New("JSON object key is not a string")
			}
			if _, duplicate := keys[key]; duplicate {
				return errors.New("duplicate JSON key")
			}
			keys[key] = struct{}{}
			if err := consumeJSONValue(decoder, depth+1); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil || closing != json.Delim('}') {
			return errors.New("JSON object has invalid closing delimiter")
		}
	case '[':
		for decoder.More() {
			if err := consumeJSONValue(decoder, depth+1); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil || closing != json.Delim(']') {
			return errors.New("JSON array has invalid closing delimiter")
		}
	default:
		return errors.New("JSON contains an unexpected delimiter")
	}
	return nil
}
