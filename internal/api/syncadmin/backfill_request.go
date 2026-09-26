package syncadmin

import (
	"fmt"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/api/pytime"
)

// backfillWindow is BackfillRequest after its validators: the window and the
// optional scope of a backfill, whichever form the caller used.
type backfillWindow struct {
	// Since and Before are the resolved instants (UTC): the structured
	// selector's own, or, for the legacy flat fields, the first instant of the
	// `since` calendar date and the LAST microsecond of the `before` date
	// (Python: datetime.combine(before, time.max)).
	Since, Before time.Time
	// Structured is true for the `selector` form. It changes what the response
	// and the history row carry: the legacy dates are inclusive calendar dates,
	// the selector's instants are half-open.
	Structured bool
	// LegacySince and LegacyBefore are the flat dates (zero for the selector form).
	LegacySince, LegacyBefore time.Time
	// SinceAt and BeforeAt are the selector's own aware datetimes, offset kept:
	// Python reads .date() and .isoformat() from them in their own timezone.
	SinceAt, BeforeAt pytime.DateTime
	// SourceIDs and DatasetKeys are the selector's optional scope; the Set flags
	// tell an omitted or null list (every enabled one) from a given one.
	SourceIDs, DatasetKeys       []string
	SourceIDsSet, DatasetKeysSet bool
}

// valueErrorAt is the FastAPI rendering of a ValueError a model_validator
// raised: ctx.error is the exception, which jsonable_encoder renders as {}.
func valueErrorAt(loc []pyjson.Value, input pyjson.Value, message string) pybody.Error {
	ctx := pyjson.NewObject()
	ctx.Set("error", pyjson.NewObject())
	return pybody.Error{Type: "value_error", Loc: loc, Msg: "Value error, " + message, Input: input, Ctx: ctx}
}

func stringListOf(model pybody.Model, name string) ([]string, bool, bool) {
	field := pybody.Get(model, name, pybody.Nullable, pybody.StrList)
	return field.Value, field.Set && !field.Null, field.OK
}

// parseSelectorModel is BackfillSelectorRequest: since and before are aware
// datetimes and before must be after since; source_ids and dataset_keys are
// optional string lists.
func parseSelectorModel(e *pybody.Errors, raw pyjson.Value, loc []pyjson.Value) (backfillWindow, bool) {
	object, isObject := raw.(*pyjson.Object)
	if !isObject {
		*e = append(*e, pybody.Error{Type: "model_attributes_type", Loc: loc,
			Msg: "Input should be a valid dictionary or object to extract fields from", Input: raw})
		return backfillWindow{}, false
	}
	model := pybody.Model{Errors: e, Object: object, Loc: loc}
	since := pybody.Get(model, "since", pybody.Required, pybody.AwareDatetime)
	before := pybody.Get(model, "before", pybody.Required, pybody.AwareDatetime)
	var out backfillWindow
	var ok1, ok2 bool
	out.SourceIDs, out.SourceIDsSet, ok1 = stringListOf(model, "source_ids")
	out.DatasetKeys, out.DatasetKeysSet, ok2 = stringListOf(model, "dataset_keys")
	if !(since.OK && before.OK && ok1 && ok2) {
		return backfillWindow{}, false
	}
	// The model_validator(mode="after") runs only on a model whose fields all
	// validated: an aware-datetime comparison is by instant.
	if !since.Value.Time.Before(before.Value.Time) {
		*e = append(*e, valueErrorAt(loc, raw, "backfill selector before must be after since"))
		return backfillWindow{}, false
	}
	out.Since, out.Before, out.Structured = since.Value.Time.UTC(), before.Value.Time.UTC(), true
	out.SinceAt, out.BeforeAt = since.Value, before.Value
	return out, true
}

// parseBackfillRequest is BackfillRequest: a structured selector, or the legacy
// flat calendar dates `since` and `before`, never both, one of the two required.
func parseBackfillRequest(body pybody.Body) (backfillWindow, pybody.Errors) {
	var problems pybody.Errors
	object, ok := problems.Object(body)
	if !ok {
		return backfillWindow{}, problems
	}
	model := pybody.Model{Errors: &problems, Object: object, Loc: []pyjson.Value{"body"}}
	selector := pybody.Get(model, "selector", pybody.Nullable, parseSelectorModel)
	since := pybody.Get(model, "since", pybody.Nullable, pybody.Date)
	before := pybody.Get(model, "before", pybody.Nullable, pybody.Date)
	if !(selector.OK && since.OK && before.OK) {
		return backfillWindow{}, problems
	}
	// model_validator(mode="after") runs only on a body whose fields all validated.
	selectorGiven := selector.Set && !selector.Null
	sinceGiven := since.Set && !since.Null
	beforeGiven := before.Set && !before.Null
	switch {
	case selectorGiven && (sinceGiven || beforeGiven):
		problems = append(problems, valueErrorAt([]pyjson.Value{"body"}, object, "backfill selector cannot be mixed with legacy flat fields"))
		return backfillWindow{}, problems
	case selectorGiven:
		return selector.Value, problems
	case !sinceGiven || !beforeGiven:
		problems = append(problems, valueErrorAt([]pyjson.Value{"body"}, object, "backfill requires since and before, either top-level or in selector"))
		return backfillWindow{}, problems
	}
	legacySince, legacyBefore := since.Value.UTC(), before.Value.UTC()
	return backfillWindow{
		Since:        time.Date(legacySince.Year(), legacySince.Month(), legacySince.Day(), 0, 0, 0, 0, time.UTC),
		Before:       time.Date(legacyBefore.Year(), legacyBefore.Month(), legacyBefore.Day(), 23, 59, 59, 999999000, time.UTC),
		LegacySince:  legacySince,
		LegacyBefore: legacyBefore,
	}, problems
}

// wall is the datetime's wall clock in its own offset.
func wall(value pytime.DateTime) time.Time {
	return value.Time.Add(time.Duration(value.Offset)*time.Second + time.Duration(value.OffsetMicro)*time.Microsecond).UTC()
}

// dateOf is datetime.date(): the calendar date of the wall clock, midnight UTC.
func dateOf(clock time.Time) time.Time {
	return time.Date(clock.Year(), clock.Month(), clock.Day(), 0, 0, 0, 0, time.UTC)
}

// isoOf is datetime.isoformat() of an aware datetime: microseconds only when
// non-zero, then "+HH:MM" (or "-HH:MM"; whole minutes are all a request carries).
func isoOf(value pytime.DateTime) string {
	clock := wall(value)
	text := clock.Format("2006-01-02T15:04:05")
	if micro := clock.Nanosecond() / 1000; micro != 0 {
		text += fmt.Sprintf(".%06d", micro)
	}
	offset := value.Offset
	sign := "+"
	if offset < 0 {
		sign, offset = "-", -offset
	}
	return text + fmt.Sprintf("%s%02d:%02d", sign, offset/3600, offset%3600/60)
}

// sinceISO and beforeISO are the response's `since` and `before`: the selector's
// own isoformat, or the flat date's.
func (w backfillWindow) sinceISO() string {
	if w.Structured {
		return isoOf(w.SinceAt)
	}
	return w.LegacySince.Format("2006-01-02")
}

func (w backfillWindow) beforeISO() string {
	if w.Structured {
		return isoOf(w.BeforeAt)
	}
	return w.LegacyBefore.Format("2006-01-02")
}

// historySince and historyBefore are the BackfillJob row's inclusive calendar
// dates: the selector's since date and the date of its last included microsecond
// (both in the selector's own timezone), or the flat dates.
func (w backfillWindow) historySince() time.Time {
	if w.Structured {
		return dateOf(wall(w.SinceAt))
	}
	return w.LegacySince
}

func (w backfillWindow) historyBefore() time.Time {
	if w.Structured {
		return dateOf(wall(w.BeforeAt).Add(-time.Microsecond))
	}
	return w.LegacyBefore
}

// days is (selector.before - selector.since).days: timedelta's floor division,
// so a negative span (the flat dates, since after before) rounds down.
func (w backfillWindow) days() int64 {
	// UnixMicro, not Sub: time.Duration saturates near 292 years, and a flat
	// window may span years 1 to 9999.
	span := w.Before.UnixMicro() - w.Since.UnixMicro()
	const day = int64(24 * time.Hour / time.Microsecond)
	quotient := span / day
	if span%day != 0 && span < 0 {
		quotient--
	}
	return quotient
}
