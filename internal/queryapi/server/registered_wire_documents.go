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
