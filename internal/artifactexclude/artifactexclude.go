// Package artifactexclude is the per-sync-config list of CI artifact name prefixes the GitHub tests sync skips
// (sync_options.ci_artifact_exclude_prefixes, copied to the integration's config, which every sync unit of the
// integration reads). The admin API and `dho admin sync-configs artifact-excludes` both validate with Parse and write
// with WriteTx: one validator and one writer for both callers.
package artifactexclude

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/pydict"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// Key is the option key in sync_configurations.sync_options and in integrations.config.
const Key = "ci_artifact_exclude_prefixes"

// MaxEntries bounds the list: a deployment names a few artifact families, and the route matches every artifact name
// against every entry.
const MaxEntries = 16

// MaxEntryBytes bounds one prefix: GitHub artifact names are at most 255 characters and a prefix needs far less.
const MaxEntryBytes = 100

// Change is what a valid value asks for: Clear removes the key, otherwise Prefixes is stored as given.
type Change struct {
	Clear    bool
	Prefixes []string
}

// Refusal is a value or a target the field does not accept. Its message never holds the submitted value.
type Refusal struct {
	Message string
	// Index is the offending entry (0-based), or -1 when the refusal is not about one entry.
	Index int
}

func (r *Refusal) Error() string { return r.Message }

func refusal(index int, format string, args ...any) *Refusal {
	return &Refusal{Message: Key + ": " + fmt.Sprintf(format, args...), Index: index}
}

// ErrConfigNotFound: no sync configuration of the organization has the id.
var ErrConfigNotFound = errors.New("artifactexclude: sync configuration not found")

// Parse validates a submitted value for a configuration of provider. JSON null and [] clear the key. Every other
// value must be a list of 1..MaxEntries distinct (case-insensitive) prefixes of 1..MaxEntryBytes bytes from
// [A-Za-z0-9._-]; nothing is trimmed, so the stored list is the sent list. Only GitHub reads the field: any other
// provider is refused.
func Parse(provider string, value pyjson.Value) (Change, error) {
	if strings.ToLower(provider) != "github" {
		return Change{}, refusal(-1, "only a github sync configuration reads this field")
	}
	switch typed := value.(type) {
	case nil:
		return Change{Clear: true}, nil
	case []pyjson.Value:
		items := make([]any, len(typed))
		for index, item := range typed {
			items[index] = item
		}
		return parseItems(items)
	}
	return Change{}, refusal(-1, "must be a list of strings or null")
}

// ParseDecoded is Parse for a value decoded by encoding/json (a sync unit's integration config), for GitHub.
func ParseDecoded(value any) (Change, error) {
	switch typed := value.(type) {
	case nil:
		return Change{Clear: true}, nil
	case []any:
		return parseItems(typed)
	}
	return Change{}, refusal(-1, "must be a list of strings or null")
}

func parseItems(items []any) (Change, error) {
	if len(items) == 0 {
		return Change{Clear: true}, nil
	}
	if len(items) > MaxEntries {
		return Change{}, refusal(-1, "holds more than %d entries", MaxEntries)
	}
	prefixes := make([]string, len(items))
	seen := make(map[string]int, len(items))
	for index, item := range items {
		text, ok := item.(string)
		if !ok {
			return Change{}, refusal(index, "entry %d is not a string", index)
		}
		if text == "" {
			return Change{}, refusal(index, "entry %d is empty; an empty prefix would skip every artifact", index)
		}
		if len(text) > MaxEntryBytes {
			return Change{}, refusal(index, "entry %d is longer than %d bytes", index, MaxEntryBytes)
		}
		if !allowedText(text) {
			return Change{}, refusal(index, "entry %d holds a character outside A-Z a-z 0-9 . _ -", index)
		}
		folded := strings.ToLower(text)
		if first, dup := seen[folded]; dup {
			return Change{}, refusal(index, "entry %d repeats entry %d (case-insensitive)", index, first)
		}
		seen[folded] = index
		prefixes[index] = text
	}
	return Change{Prefixes: prefixes}, nil
}

func allowedText(text string) bool {
	for index := 0; index < len(text); index++ {
		switch c := text[index]; {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '.', c == '_', c == '-':
		default:
			return false
		}
	}
	return true
}

// Matches reports whether name starts with one of prefixes, case-insensitively.
func Matches(name string, prefixes []string) bool {
	lower := strings.ToLower(name)
	for _, prefix := range prefixes {
		if strings.HasPrefix(lower, strings.ToLower(prefix)) {
			return true
		}
	}
	return false
}

// Result is what WriteTx did. The ids are 8-character prefixes only.
type Result struct {
	ConfigIDPrefix, OrgIDPrefix, IntegrationIDPrefix string
	SyncOptionsChanged, IntegrationConfigChanged     bool
	// SyncOptions is the configuration's stored sync_options text after the write.
	SyncOptions string
}

// WriteTx applies change to ONE key of the configuration's sync_options and of its integration's config, on tx,
// leaving every other key of both documents as it is. A row is updated (with updated_at = now) only when its document
// changed. The configuration must belong to org, be a GitHub configuration and have an integration.
func WriteTx(ctx context.Context, tx pgx.Tx, org string, configID uuid.UUID, change Change, now time.Time) (Result, error) {
	var provider string
	var integrationID *uuid.UUID
	var optionsText *string
	err := tx.QueryRow(ctx, `SELECT provider, integration_id, sync_options::text FROM sync_configurations
WHERE org_id = $1 AND id = $2 FOR UPDATE`, org, configID).Scan(&provider, &integrationID, &optionsText)
	if errors.Is(err, pgx.ErrNoRows) {
		return Result{}, ErrConfigNotFound
	}
	if err != nil {
		return Result{}, fmt.Errorf("read sync configuration: %w", err)
	}
	if strings.ToLower(provider) != "github" {
		return Result{}, refusal(-1, "only a github sync configuration reads this field")
	}
	if integrationID == nil {
		return Result{}, refusal(-1, "the sync configuration has no integration")
	}
	var configText *string
	if err := tx.QueryRow(ctx, `SELECT config::text FROM integrations WHERE org_id = $1 AND id = $2 FOR UPDATE`,
		org, *integrationID).Scan(&configText); err != nil {
		return Result{}, fmt.Errorf("read integration: %w", err)
	}
	result := Result{ConfigIDPrefix: idPrefix(configID.String()), OrgIDPrefix: idPrefix(org), IntegrationIDPrefix: idPrefix(integrationID.String())}
	nextOptions, optionsChanged, err := apply(optionsText, change)
	if err != nil {
		return Result{}, fmt.Errorf("sync_options: %w", err)
	}
	nextConfig, configChanged, err := apply(configText, change)
	if err != nil {
		return Result{}, fmt.Errorf("integration config: %w", err)
	}
	if optionsChanged {
		if _, err := tx.Exec(ctx, `UPDATE sync_configurations SET sync_options = $2::json, updated_at = $3 WHERE id = $1`,
			configID, nextOptions, now); err != nil {
			return Result{}, fmt.Errorf("update sync configuration: %w", err)
		}
	}
	if configChanged {
		if _, err := tx.Exec(ctx, `UPDATE integrations SET config = $2::json, updated_at = $3 WHERE id = $1`,
			*integrationID, nextConfig, now); err != nil {
			return Result{}, fmt.Errorf("update integration: %w", err)
		}
	}
	result.SyncOptionsChanged, result.IntegrationConfigChanged, result.SyncOptions = optionsChanged, configChanged, nextOptions
	return result, nil
}

// apply sets or deletes Key in the stored document and dumps it as the admin API stores options (json.dumps).
func apply(stored *string, change Change) (string, bool, error) {
	var current pyjson.Value
	if stored != nil {
		value, err := pyjson.DecodeString(*stored)
		if err != nil {
			return "", false, err
		}
		current = value
	}
	document, err := pydict.Plain(current)
	if err != nil {
		return "", false, err
	}
	old, had := document.Get(Key)
	changed := had
	if change.Clear {
		document.Delete(Key)
	} else {
		items := make([]pyjson.Value, len(change.Prefixes))
		for index, prefix := range change.Prefixes {
			items[index] = prefix
		}
		changed = !had || !pyjson.Equal(old, items)
		document.Set(Key, items)
	}
	if !changed {
		if stored == nil {
			return "", false, nil
		}
		return *stored, false, nil
	}
	text, err := pyjson.Dumps(document)
	if err != nil {
		return "", false, err
	}
	return text, true, nil
}

func idPrefix(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}
