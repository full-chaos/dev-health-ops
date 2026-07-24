package remaining

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"time"

	"github.com/google/uuid"
)

// ScopeVersion is deliberately embedded in every durable partition scope so
// the Python bridge and Go producer never infer a changing default.
const ScopeVersion = 1

type capacityScope struct {
	Version     int     `json:"version"`
	TeamID      *string `json:"team_id,omitempty"`
	WorkScopeID *string `json:"work_scope_id,omitempty"`
	TargetItems *int    `json:"target_items,omitempty"`
	TargetDate  *string `json:"target_date,omitempty"`
	HistoryDays int     `json:"history_days"`
	Simulations int     `json:"simulations"`
	AllTeams    bool    `json:"all_teams"`
}
type complexityScope struct {
	Version       int      `json:"version"`
	Day           string   `json:"day"`
	BackfillDays  int      `json:"backfill_days"`
	RepoID        *string  `json:"repo_id,omitempty"`
	SearchPattern *string  `json:"search_pattern,omitempty"`
	LanguageGlobs []string `json:"language_globs,omitempty"`
	ExcludeGlobs  []string `json:"exclude_globs,omitempty"`
	MaxFiles      *int     `json:"max_files,omitempty"`
}
type doraScope struct {
	Version      int     `json:"version"`
	Day          string  `json:"day"`
	BackfillDays int     `json:"backfill_days"`
	RepoID       *string `json:"repo_id,omitempty"`
	RepoName     *string `json:"repo_name,omitempty"`
	Sink         string  `json:"sink"`
	Metrics      *string `json:"metrics,omitempty"`
	Interval     string  `json:"interval"`
}
type releaseImpactScope struct {
	Version                 int    `json:"version"`
	Day                     string `json:"day"`
	BackfillDays            int    `json:"backfill_days"`
	RecomputationWindowDays int    `json:"recomputation_window_days"`
}
type recommendationsScope struct {
	Version int     `json:"version"`
	Window  int     `json:"window"`
	TeamID  *string `json:"team_id,omitempty"`
	AsOf    *string `json:"as_of,omitempty"`
}
type membershipScope struct {
	Version int      `json:"version"`
	RepoIDs []string `json:"repo_ids,omitempty"`
}
type extraMetricsScope struct {
	Version      int    `json:"version"`
	Day          string `json:"day"`
	BackfillDays int    `json:"backfill_days"`
}
type teamMetricsScope struct {
	Version      int    `json:"version"`
	Day          string `json:"day"`
	BackfillDays int    `json:"backfill_days"`
}

func validateFamilyScope(family string, raw json.RawMessage) (json.RawMessage, error) {
	switch family {
	case "capacity":
		var value capacityScope
		if err := strictScope(raw, &value); err != nil {
			return nil, err
		}
		if value.Version != ScopeVersion || value.HistoryDays < 1 || value.HistoryDays > 365 || value.Simulations < 100 || value.Simulations > 100000 || (value.AllTeams && (value.TeamID != nil || value.WorkScopeID != nil)) || (!value.AllTeams && value.TeamID == nil && value.WorkScopeID == nil) || !optionalUUID(value.TeamID) || !boundedOptional(value.WorkScopeID, 256) || !optionalPositive(value.TargetItems) || !optionalDate(value.TargetDate) {
			return nil, errors.New("invalid capacity scope")
		}
		return json.Marshal(value)
	case "complexity":
		var value complexityScope
		if err := strictScope(raw, &value); err != nil {
			return nil, err
		}
		if value.Version != ScopeVersion || !validDate(value.Day) || value.BackfillDays != 1 || !optionalUUID(value.RepoID) || !boundedStrings(value.LanguageGlobs, 32, 256) || !boundedStrings(value.ExcludeGlobs, 32, 256) || !optionalPositive(value.MaxFiles) || !boundedOptional(value.SearchPattern, 256) {
			return nil, errors.New("invalid complexity scope")
		}
		return json.Marshal(value)
	case "dora":
		var value doraScope
		if err := strictScope(raw, &value); err != nil {
			return nil, err
		}
		if value.Version != ScopeVersion || !validDate(value.Day) || value.BackfillDays < 1 || value.BackfillDays > 90 || !optionalUUID(value.RepoID) || !boundedOptional(value.RepoName, 256) || (value.Sink != "auto" && value.Sink != "clickhouse") || (value.Interval != "daily" && value.Interval != "weekly" && value.Interval != "monthly") || !boundedOptional(value.Metrics, 256) {
			return nil, errors.New("invalid dora scope")
		}
		return json.Marshal(value)
	case "release_impact":
		var value releaseImpactScope
		if err := strictScope(raw, &value); err != nil {
			return nil, err
		}
		if value.Version != ScopeVersion || !validDate(value.Day) || value.BackfillDays < 1 || value.BackfillDays > 90 || value.RecomputationWindowDays < 1 || value.RecomputationWindowDays > 30 {
			return nil, errors.New("invalid release impact scope")
		}
		return json.Marshal(value)
	case "recommendations":
		var value recommendationsScope
		if err := strictScope(raw, &value); err != nil {
			return nil, err
		}
		if value.Version != ScopeVersion || value.Window < 1 || value.Window > 90 || !optionalUUID(value.TeamID) || !optionalDate(value.AsOf) {
			return nil, errors.New("invalid recommendations scope")
		}
		return json.Marshal(value)
	case "membership_backfill":
		var value membershipScope
		if err := strictScope(raw, &value); err != nil {
			return nil, err
		}
		if value.Version != ScopeVersion || len(value.RepoIDs) > 256 || !allUUID(value.RepoIDs) {
			return nil, errors.New("invalid membership scope")
		}
		return json.Marshal(value)
	case "extra_metrics":
		var value extraMetricsScope
		if err := strictScope(raw, &value); err != nil {
			return nil, err
		}
		if value.Version != ScopeVersion || !validDate(value.Day) || value.BackfillDays < 1 || value.BackfillDays > 30 {
			return nil, errors.New("invalid extra metrics scope")
		}
		return json.Marshal(value)
	case "team_metrics":
		var value teamMetricsScope
		if err := strictScope(raw, &value); err != nil {
			return nil, err
		}
		if value.Version != ScopeVersion || !validDate(value.Day) || value.BackfillDays < 1 || value.BackfillDays > 30 {
			return nil, errors.New("invalid team metrics scope")
		}
		return json.Marshal(value)
	}
	return nil, errors.New("unknown remaining metrics family")
}

func strictScope(raw json.RawMessage, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("multiple JSON values")
	}
	return nil
}
func validDate(value string) bool     { _, err := time.Parse("2006-01-02", value); return err == nil }
func optionalDate(value *string) bool { return value == nil || validDate(*value) }
func optionalUUID(value *string) bool {
	if value == nil {
		return true
	}
	parsed, err := uuid.Parse(*value)
	return err == nil && parsed.String() == *value
}
func optionalPositive(value *int) bool { return value == nil || (*value > 0 && *value <= 1000000) }
func boundedOptional(value *string, max int) bool {
	return value == nil || (len(*value) > 0 && len(*value) <= max)
}
func boundedStrings(values []string, maxCount, maxLength int) bool {
	if len(values) > maxCount {
		return false
	}
	for _, value := range values {
		if len(value) == 0 || len(value) > maxLength {
			return false
		}
	}
	return true
}
func allUUID(values []string) bool {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		parsed, err := uuid.Parse(value)
		if err != nil || parsed.String() != value {
			return false
		}
		if _, duplicate := seen[value]; duplicate {
			return false
		}
		seen[value] = struct{}{}
	}
	return true
}
