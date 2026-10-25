package integrationsadmin

import (
	"net/http"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/api/pytime"
	schedsync "github.com/full-chaos/dev-health-ops/internal/scheduler/sync"
	"github.com/full-chaos/dev-health-ops/internal/synchandoff"
)

// backfillBody is BackfillTriggerRequest after its model validator: the
// window and scope of the run, whichever form the caller used.
type backfillBody struct {
	scope         triggerScope
	since, before pytime.DateTime
}

// valueError is the FastAPI rendering of a ValueError raised in a
// model_validator: ctx.error is the exception, which jsonable_encoder renders
// as {}. The model's own location is the body, and its input the whole body.
func valueError(input pyjson.Value, message string) pybody.Error {
	ctx := pyjson.NewObject()
	ctx.Set("error", pyjson.NewObject())
	return pybody.Error{Type: "value_error", Loc: []pyjson.Value{"body"}, Msg: "Value error, " + message, Input: input, Ctx: ctx}
}

// stringListField is `name: list[str] | None = None`.
func stringListField(model pybody.Model, name string) ([]string, bool, bool) {
	field := pybody.Get(model, name, pybody.Nullable, pybody.StrList)
	return field.Value, field.Set && !field.Null, field.OK
}

// datetimeField is `name: datetime | None = None`, or `name: datetime` when
// required.
func datetimeField(model pybody.Model, name string, kind pybody.Kind) pybody.Field[pytime.DateTime] {
	return pybody.Get(model, name, kind, pybody.Datetime)
}

// parseSelector is BackfillSelector: since and before are required.
func parseSelector(model pybody.Model) (backfillBody, bool) {
	var out backfillBody
	since := datetimeField(model, "since", pybody.Required)
	before := datetimeField(model, "before", pybody.Required)
	var ok1, ok2 bool
	out.scope.sourceIDs, out.scope.sourceIDsSet, ok1 = stringListField(model, "source_ids")
	out.scope.datasetKeys, out.scope.datasetKeysSet, ok2 = stringListField(model, "dataset_keys")
	out.since, out.before = since.Value, before.Value
	return out, since.OK && before.OK && ok1 && ok2
}

func parseBackfill(body pybody.Body) (backfillBody, pybody.Errors) {
	var problems pybody.Errors
	object, ok := problems.Object(body)
	if !ok {
		return backfillBody{}, problems
	}
	model := pybody.Model{Errors: &problems, Object: object, Loc: []pyjson.Value{"body"}}
	selector := pybody.Get(model, "selector", pybody.Nullable, func(e *pybody.Errors, raw pyjson.Value, loc []pyjson.Value) (backfillBody, bool) {
		nested, isObject := raw.(*pyjson.Object)
		if !isObject {
			*e = append(*e, pybody.Error{Type: "model_attributes_type", Loc: loc,
				Msg: "Input should be a valid dictionary or object to extract fields from", Input: raw})
			return backfillBody{}, false
		}
		return parseSelector(pybody.Model{Errors: e, Object: nested, Loc: loc})
	})
	since := datetimeField(model, "since", pybody.Nullable)
	before := datetimeField(model, "before", pybody.Nullable)
	var flat backfillBody
	var ok1, ok2 bool
	flat.scope.sourceIDs, flat.scope.sourceIDsSet, ok1 = stringListField(model, "source_ids")
	flat.scope.datasetKeys, flat.scope.datasetKeysSet, ok2 = stringListField(model, "dataset_keys")
	if !(selector.OK && since.OK && before.OK && ok1 && ok2) {
		return backfillBody{}, problems
	}
	// The model_validator(mode="after") runs only on a body whose fields
	// all validated.
	selectorGiven := selector.Set && !selector.Null
	flatGiven := (since.Set && !since.Null) || (before.Set && !before.Null) || flat.scope.sourceIDsSet || flat.scope.datasetKeysSet
	switch {
	case selectorGiven && flatGiven:
		problems = append(problems, valueError(object, "backfill selector cannot be mixed with legacy flat fields"))
		return backfillBody{}, problems
	case selectorGiven:
		return selector.Value, problems
	case !(since.Set && !since.Null) || !(before.Set && !before.Null):
		problems = append(problems, valueError(object, "backfill requires since and before, either top-level or in selector"))
		return backfillBody{}, problems
	}
	flat.since, flat.before = since.Value, before.Value
	return flat, problems
}

// backfillTrigger is POST /integrations/{integration_id}/backfill. It writes
// the occurrence and its trigger and answers 202 at once; the scheduler plans
// the run, and the occurrence is the handle to it.
func (h handlers) backfillTrigger(w http.ResponseWriter, r *http.Request) {
	body, _ := policy.BodyFrom(r.Context())
	request, problems := parseBackfill(body)
	if len(problems) > 0 {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(problems), nil)
		return
	}
	var since, before time.Time = request.since.Time.UTC(), request.before.Time.UTC()
	trigger, err := h.mintHandOff(r, request.scope, "backfill", "backfill", &since, &before, func(target handoffScope) error {
		return checkBackfillWindow(request.scope, target, since, before)
	})
	if err != nil {
		h.fail(w, r, "hand off the backfill", err)
		return
	}
	out := pyjson.NewObject()
	out.Set("status", "accepted")
	out.Set("integration_id", r.PathValue("integration_id"))
	out.Set("occurrence_id", trigger.OccurrenceID)
	out.Set("task_id", "occurrence:"+trigger.OccurrenceID)
	policy.WriteModel(w, http.StatusAccepted, out, nil)
}

// checkBackfillWindow refuses, before any write, a window whose since is not
// before its before (Python's planner refused it too) and a window and scope the
// scheduler would refuse over its unit cap.
func checkBackfillWindow(scope triggerScope, target handoffScope, since, before time.Time) error {
	if !since.Before(before) {
		return refuse(http.StatusBadRequest, "Backfill since must be before")
	}
	if scope.datasetKeysSet && len(scope.datasetKeys) == 0 || scope.sourceIDsSet && len(scope.sourceIDs) == 0 {
		return nil
	}
	sources := len(target.sources)
	if scope.sourceIDsSet {
		sources = len(scope.sourceIDs)
	}
	days := (before.UnixMicro() - since.UnixMicro()) / int64(24*time.Hour/time.Microsecond)
	if err := synchandoff.CheckBackfillScale(days, int64(sources), int64(schedsync.UnitCapFromEnv())); err != nil {
		return refuse(http.StatusUnprocessableEntity, err.Error())
	}
	return nil
}
