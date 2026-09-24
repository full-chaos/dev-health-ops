package syncadmin

import (
	"errors"
	"strconv"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
	schedsync "github.com/full-chaos/dev-health-ops/internal/scheduler/sync"
)

// The sync config write paths' shared validation engines (create, batch
// create and update run the same checks): the schedule's cron interval, the
// schedule's timezone, and the auto-import category flags.

// errCronUnsupported is the Go-authored 422 text for an expression outside
// the scheduler's cron domain.
var errCronUnsupported = errors.New("only five-field expressions (minute hour day-of-month month day-of-week) are supported")

// errCronNotString is the Go-authored 422 text for a schedule_cron that is
// not a string.
var errCronNotString = errors.New("the expression must be a string")

// cronIntervalHours is create_sync_config's
// `Croniter(schedule_cron).get_next(float)` twice: the hours between the
// first two occurrences after now, in UTC (croniter reads a float start as
// a naive UTC instant).
//
// Named divergence (lead ruling): the accepted domain is the Go scheduler's
// (scheduler/sync.NextOccurrence), which will run the schedule; croniter
// also accepts six- and seven-field expressions, @aliases, R, mixed L/W/#,
// "?" and non-ASCII decimal digits (its fields go through Python's int()).
// Those, and a value that is not a string, are refused here with a
// Go-authored message, where Python accepts or refuses with croniter's own
// text. Every expression both accept has the same interval.
func cronIntervalHours(expression pyjson.Value, now time.Time) (float64, error) {
	text, ok := expression.(string)
	if !ok {
		return 0, errCronNotString
	}
	first, _, err := schedsync.NextOccurrence(text, now, "")
	if err != nil {
		return 0, cronError(err)
	}
	second, _, err := schedsync.NextOccurrence(text, first, "")
	if err != nil {
		return 0, cronError(err)
	}
	// (next2 - next1) / 3600.0 over whole-second floats, as Python divides.
	return float64(second.Unix()-first.Unix()) / 3600.0, nil
}

func cronError(err error) error {
	if errors.Is(err, schedsync.ErrUnsupportedCron) {
		return errCronUnsupported
	}
	return err
}

// cronInvalidDetail is the 422 detail "Invalid cron expression: {exc}".
func cronInvalidDetail(err error) string {
	return "Invalid cron expression: " + err.Error()
}

// cronIntervalRefusal is the 403 detail when the interval is below the
// tier's min_sync_interval_hours: f"Sync interval {interval_hours:.2f}h is
// below the minimum {min_interval}h allowed for your tier", min_interval
// being float(get_limit(...)).
func cronIntervalRefusal(intervalHours, minInterval float64) string {
	return "Sync interval " + strconv.FormatFloat(intervalHours, 'f', 2, 64) + "h is below the minimum " +
		pythonparity.Repr(minInterval) + "h allowed for your tier"
}

// errTimezoneNotString is validate_timezone_name's TypeError for a truthy
// value that is not a str (ZoneInfo(5)); the route answers it with its
// bare 500, as Python's uncaught TypeError does.
var errTimezoneNotString = errors.New("validate_timezone_name: timezone is not a str")

// validateTimezoneName is utils/datetime.validate_timezone_name: a falsy
// value passes; a str must be a zone ZoneInfo builds, else
// ValueError(f"Invalid timezone: {tz_name!r}") (returned as its detail
// text); any other truthy value raises TypeError.
func validateTimezoneName(value pyjson.Value) (string, error) {
	if !pyjson.Truthy(value) {
		return "", nil
	}
	name, ok := value.(string)
	if !ok {
		return "", errTimezoneNotString
	}
	if pythonparity.ZoneInfoKeyValid(name) {
		return "", nil
	}
	return "Invalid timezone: " + pythonparity.StrRepr(name), nil
}

// autoImportCategory is one of _CATEGORY_TO_SYNC_OPTION's entries, in its
// order.
type autoImportCategory struct{ category, option string }

var autoImportCategories = []autoImportCategory{
	{"teams", "auto_import_teams"},
	{"projects", "auto_import_projects"},
	{"members", "auto_import_members"},
}

const unsupportedAutoImportProviderReason = "provider does not support team/project/member auto-import"

// autoImportCapability is one AutoImportCapability.
type autoImportCapability struct {
	supports map[string]bool
	reasons  [][2]string
}

// autoImportCapabilityProviders is _AUTO_IMPORT_CAPABILITIES in its dict
// order.
var autoImportCapabilityProviders = []string{"github", "gitlab", "jira", "linear"}

var autoImportCapabilityByProvider = map[string]autoImportCapability{
	"github": {
		supports: map[string]bool{"teams": true, "projects": false, "members": true},
		reasons:  [][2]string{{"projects", "GitHub attributes ownership via repos, not projects."}},
	},
	"gitlab": {supports: map[string]bool{"teams": true, "projects": true, "members": true}},
	"jira":   {supports: map[string]bool{"teams": true, "projects": true, "members": true}},
	"linear": {supports: map[string]bool{"teams": true, "projects": true, "members": true}},
}

// autoImportCapabilityFor is auto_import_capabilities(provider): the entry
// for provider.strip().lower(), else every category unsupported.
func autoImportCapabilityFor(provider string) autoImportCapability {
	if capability, ok := autoImportCapabilityByProvider[pythonparity.Lower(pythonparity.Strip(provider))]; ok {
		return capability
	}
	reasons := make([][2]string, 0, len(autoImportCategories))
	for _, category := range autoImportCategories {
		reasons = append(reasons, [2]string{category.category, unsupportedAutoImportProviderReason})
	}
	return autoImportCapability{supports: map[string]bool{}, reasons: reasons}
}

func (capability autoImportCapability) reason(category string) string {
	for _, reason := range capability.reasons {
		if reason[0] == category {
			return reason[1]
		}
	}
	return unsupportedAutoImportProviderReason
}

// unsupportedAutoImportCategories is
// providers/team_capabilities.unsupported_auto_import_categories: category
// -> reason for every category whose sync_options flag is truthy and that
// the provider cannot supply, in category order.
func unsupportedAutoImportCategories(provider string, options *pyjson.Object) *pyjson.Object {
	capability := autoImportCapabilityFor(provider)
	out := pyjson.NewObject()
	for _, category := range autoImportCategories {
		value, _ := options.Get(category.option)
		if !pyjson.Truthy(value) {
			continue
		}
		if !capability.supports[category.category] {
			out.Set(category.category, capability.reason(category.category))
		}
	}
	return out
}

// malformedAutoImportCategoryValues is
// malformed_auto_import_category_values rendered as the 422 detail does:
// option key -> str(value) for every flag present that is not a bool, in
// option order.
func malformedAutoImportCategoryValues(options *pyjson.Object) *pyjson.Object {
	out := pyjson.NewObject()
	for _, category := range autoImportCategories {
		value, present := options.Get(category.option)
		if !present {
			continue
		}
		if _, isBool := value.(bool); isBool {
			continue
		}
		out.Set(category.option, pyjson.Str(value))
	}
	return out
}
