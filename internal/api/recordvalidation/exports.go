// Package recordvalidation is external-ingest's record and envelope
// validation: pydantic's model_validate reproduced exactly (codes, messages,
// paths, order) from the golden record-model bundle. It imports only pyjson,
// pytime, pybody and pythonparity, so a worker binary can link the shared
// exact validator without linking the HTTP api's policy layer.
package recordvalidation

const (
	// LegacyEntityFamily and OperationalEntityFamily are schemas.py's two
	// SourceDescriptor.entity_family values.
	LegacyEntityFamily      = "legacy"
	OperationalEntityFamily = "operational"

	legacyEntityFamily = LegacyEntityFamily
)

// RecordKinds returns every registered record kind, sorted.
func RecordKinds() []string { return append([]string(nil), recordKinds...) }

// KnownKind reports whether kind has a record model.
func KnownKind(kind string) bool {
	_, known := recordModels[kind]
	return known
}

// OperationalKind reports whether kind is an operational-family record kind.
func OperationalKind(kind string) bool { return operationalRecordKinds[kind] }
