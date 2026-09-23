package customerpush

import (
	_ "embed"
	"errors"
	"fmt"
	"net/http"
	"os"

	"github.com/full-chaos/dev-health-ops/internal/api/externalingest"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// adminSchemaGolden is {"envelope": BatchEnvelope.model_json_schema(
// by_alias=True), "recordKinds": {kind: model.model_json_schema(
// by_alias=True) ...}} from the real Python producer, in its key order.
// TestAdminSchemaMatchesLivePython executes that producer and compares.
//
//go:embed testdata/admin_schema.v1.json
var adminSchemaGolden []byte

func lookupEnv(name string) (string, bool) { return os.LookupEnv(name) }

// limits is _external_ingest_limits(): int() of each variable, with the
// schemas.py defaults when unset. A value int() refuses is Python's
// unhandled ValueError, the generic 500.
func (h *handlers) limits() (*pyjson.Object, error) {
	out := pyjson.NewObject()
	for _, item := range []struct{ key, name, fallback string }{
		{"maxRecordsPerBatch", "EXTERNAL_INGEST_MAX_RECORDS", fmt.Sprint(externalingest.DefaultLimits.MaxRecords)},
		{"maxBodyBytes", "EXTERNAL_INGEST_MAX_BODY_BYTES", fmt.Sprint(externalingest.DefaultLimits.MaxBodyBytes)},
	} {
		raw, ok := h.getenv(item.name)
		if !ok {
			raw = item.fallback
		}
		value, err := pythonparity.ParseInt(raw)
		if err != nil {
			return nil, fmt.Errorf("int(%s): %w", item.name, err)
		}
		out.Set(item.key, pyjson.Int{Int: value})
	}
	return out, nil
}

func (h *handlers) listSchemas(w http.ResponseWriter, r *http.Request) {
	orgID := policy.UserFrom(r.Context()).OrgID
	if !h.requireAccess(w, r, orgID) {
		return
	}
	limits, err := h.limits()
	if err != nil {
		h.internal(w, r, "read ingest limits", err)
		return
	}
	kinds := externalingest.RecordKinds()
	list := make([]pyjson.Value, len(kinds))
	for index, kind := range kinds {
		list[index] = kind
	}
	out := pyjson.NewObject()
	out.Set("schemaVersions", []pyjson.Value{externalingest.SchemaVersion})
	out.Set("recordKinds", list)
	out.Set("limits", limits)
	policy.WriteJSON(w, http.StatusOK, out, nil)
}

func (h *handlers) getSchema(w http.ResponseWriter, r *http.Request) {
	orgID := policy.UserFrom(r.Context()).OrgID
	if !h.requireAccess(w, r, orgID) {
		return
	}
	version := r.PathValue("schema_version")
	if version != externalingest.SchemaVersion {
		policy.WriteDetail(w, http.StatusNotFound, "Unknown schema version: "+pythonparity.StrRepr(version), nil)
		return
	}
	golden, err := pyjson.Decode(adminSchemaGolden)
	if err != nil {
		h.internal(w, r, "decode admin schema", err)
		return
	}
	document, ok := golden.(*pyjson.Object)
	if !ok {
		h.internal(w, r, "decode admin schema", errors.New("golden is not an object"))
		return
	}
	limits, err := h.limits()
	if err != nil {
		h.internal(w, r, "read ingest limits", err)
		return
	}
	envelope, _ := document.Get("envelope")
	kinds, _ := document.Get("recordKinds")
	out := pyjson.NewObject()
	out.Set("schemaVersion", externalingest.SchemaVersion)
	out.Set("envelope", envelope)
	out.Set("recordKinds", kinds)
	out.Set("limits", limits)
	policy.WriteJSON(w, http.StatusOK, out, nil)
}
