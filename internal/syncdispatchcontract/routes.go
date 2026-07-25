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

	DeliveryAtLeastOnce = "at_least_once"

	RouteCelery = "celery"
	RouteRiver  = "river"
)

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
