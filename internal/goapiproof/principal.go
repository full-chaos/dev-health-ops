package goapiproof

import (
	"regexp"
	"sort"
)

// PrincipalAdminProof names the org-admin proof service principal
// (edgetokenmint.AdminProofPrincipalID, CHAOS-6570): the principal whose edge access token carries the admin
// membership role. The default principal is the read-level proof principal, named by the empty string.
const PrincipalAdminProof = "admin-proof"

// operatorGatedOperations is the CHECKED-IN allowlist of operations that a read-level proof principal cannot
// execute (CHAOS-6808): each resolver refuses a viewer with an AUTHORIZATION_ERROR, so a run as the default
// principal only ever measures the refusal, and a refusal is not a proof (response_carried_graphql_errors).
// For exactly these operations, and no others, both legs (candidate and baseline go through the same edge) carry the
// admin-proof principal's edge access token.
//
// The list is closed on purpose: adding an entry is a reviewed code change, never a flag, and an operation that
// is not listed can never be widened. Every entry is a read-only query: proveRequest refuses a document that is not one
// (isMutationDocument) before any request is sent.
//
// NOT here: productTelemetryPlatformDashboard. It answers "Platform admin access required" (is_superuser), and the proof
// principals are never superusers by design (edgetokenmint.ErrPrincipalSuperuser). Proving it needs a platform-superuser
// proof principal, a new security-sensitive identity that is a separate decision.
var operatorGatedOperations = map[string]string{
	"connectorsDataHealth":  PrincipalAdminProof,
	"dataHealthIdentity":    PrincipalAdminProof,
	"mappingCoverageHealth": PrincipalAdminProof,
	"metricLineage":         PrincipalAdminProof,
}

// PrincipalFor returns the principal an operation's requests must carry: "" (the default read-level proof principal)
// for every operation that is not in the allowlist.
func PrincipalFor(operation string) string { return operatorGatedOperations[operation] }

// OperatorGatedOperations lists the allowlist, sorted, for a caller that prints or checks it.
func OperatorGatedOperations() []string {
	names := make([]string, 0, len(operatorGatedOperations))
	for name := range operatorGatedOperations {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// mutationKeyword matches the GraphQL operation types that are not a query, as a whole word anywhere in the document.
// Deliberately broad (a field or comment that says "mutation" is refused too): the widened principal is only ever
// allowed to READ, and a false refusal costs one named line while a false acceptance costs a write as an admin.
var mutationKeyword = regexp.MustCompile(`(?i)\b(mutation|subscription)\b`)

// isMutationDocument reports whether a document may write (or subscribe): true means "do not widen".
func isMutationDocument(document string) bool { return mutationKeyword.MatchString(document) }
