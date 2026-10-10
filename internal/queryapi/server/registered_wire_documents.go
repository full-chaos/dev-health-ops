package server

// webPathSmokeOperations are the registered operations the bigboy web-path smoke (dho smoke
// web-path, CHAOS-8647) sends through the web session. Each text is the wire form web sends, the
// same constant the route hashes into digestByOperation, so the smoke sends the registered
// document and never re-prints one.
var webPathSmokeOperations = map[string]string{
	"complexityTimeseries":     registeredComplexityTimeseriesDocument,
	"workGraphFlow":            registeredWorkGraphFlowDocument,
	"testopsRisk":              registeredTestopsRiskDocument,
	"home":                     registeredHomeDocument,
	"recommendations":          registeredRecommendationsDocument,
	"workItemTeamAttributions": registeredWorkItemTeamAttributionsDocument,
}

// WebPathSmokeDocument returns the registered wire-form document text for one of the operations
// the web-path smoke sends; ok is false for any other operation name.
func WebPathSmokeDocument(operation string) (string, bool) {
	text, ok := webPathSmokeOperations[operation]
	return text, ok
}

// webPathSmokeLegacyOperations are the legacy texts of the smoke's operations, oldest first, in the
// order of legacyDigestsByOperation (a test pins the two together). A legacy text is a document an
// older web still sends and this build still accepts.
var webPathSmokeLegacyOperations = map[string][]string{
	"testopsRisk": {registeredTestopsRiskV1Document},
	"home": {
		registeredHomeV1Document, registeredHomeV2Document, registeredHomeV3Document, registeredHomeV4Document,
		registeredHomeV5Document, registeredHomeV6Document, registeredHomeV7Document, registeredHomeV8Document,
	},
}

// RegisteredDocument is one document this build accepts for an operation.
type RegisteredDocument struct {
	Text string
	// Legacy is true for a text kept only so an older web still works.
	Legacy bool
}

// WebPathSmokeRegisteredDocuments returns every document this build registers for one of the
// operations the web-path smoke checks: the current text first, then the legacy texts (CHAOS-9146).
// The smoke compares the web's own document with each, so a two-step pin (this ops with the older
// web) passes as the legacy pair it is. ok is false for any other operation name.
func WebPathSmokeRegisteredDocuments(operation string) ([]RegisteredDocument, bool) {
	current, ok := webPathSmokeOperations[operation]
	if !ok {
		return nil, false
	}
	documents := []RegisteredDocument{{Text: current}}
	for _, text := range webPathSmokeLegacyOperations[operation] {
		documents = append(documents, RegisteredDocument{Text: text, Legacy: true})
	}
	return documents, true
}
