package goapiproof

// The venue-proof class (CHAOS-6099).
//
// Some operations admit only an admin-level principal (the Data Health
// page: role admin/owner/operator, or a superuser). The proof principal
// edgetokenmint mints is a viewer/member and never a superuser, so
// production's own prover can never measure them, and enable can only say
// "unproven". The owner ruled that a two-plane proof on a non-production
// venue, run as an admin test account at the SAME build production runs,
// is the enable evidence for those operations.
//
// Two mechanisms live here and nowhere else:
//
//  1. CheckVenue: the prover may take a pre-obtained login token for its
//     edge leg ONLY on a named non-production venue. The default is
//     refusal, so a production prover can never accept a login token.
//  2. VenueAdmit: enable may admit an operation the production store
//     cannot prove from a venue receipt (the prover's own JSON report) --
//     and only for the exact build, schema and document production runs,
//     only for an operation whose authorization the viewer principal
//     cannot satisfy, and only when the receipt's principal satisfies it.
//
// TRUST LEVEL of the receipt file (named limit): it is operator-supplied
// and not authenticated. It is strictly narrower than
// --acknowledge-unproven, which needs no evidence at all.

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"regexp"
	"sort"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/edgetokenmint"
)

// VenueProduction is the venue name that is refused for a login token.
const VenueProduction = "production"

// VenueAckEnvVar must hold the venue name for the prover to accept a
// login token: a second, separate act from typing the flag.
const VenueAckEnvVar = "GO_API_PROVE_VENUE_ACK"

// VenueEvidencePrefix starts review_evidence on a row enabled from a venue
// receipt; the receipt's sha256 follows.
const VenueEvidencePrefix = "VENUE-PROOF:"

// NoProdDataEvidencePrefix starts review_evidence on a row enabled under
// the "no production data" class (venue class 2); the production report's
// sha256 follows, then the VenueEvidencePrefix part.
const NoProdDataEvidencePrefix = "NO-PROD-DATA:"

// Venue classes as `status` reports them.
const (
	VenueClassAdmin  = "admin_only"
	VenueClassNoData = "no_production_data"
)

var venueEvidenceRE = regexp.MustCompile(`^(NO-PROD-DATA:[0-9a-f]{64} )?VENUE-PROOF:[0-9a-f]{64}( |$)`)

// VenueEvidence is the ONE writer of the review_evidence prefix for a
// venue-admitted row. productionDigest is empty for class 1.
func VenueEvidence(venueDigest, productionDigest, operatorEvidence string) string {
	evidence := VenueEvidencePrefix + venueDigest + " " + operatorEvidence
	if productionDigest != "" {
		evidence = NoProdDataEvidencePrefix + productionDigest + " " + evidence
	}
	return evidence
}

// VenueEvidenceClass is the ONE reader: the class a row's review_evidence
// claims, or "" for anything else (including an ACKNOWLEDGED-UNPROVEN
// waiver, whose prefix comes first). It reads the row's own text; it does
// not prove a receipt exists.
func VenueEvidenceClass(evidence string) string {
	if !venueEvidenceRE.MatchString(evidence) {
		return ""
	}
	if strings.HasPrefix(evidence, NoProdDataEvidencePrefix) {
		return VenueClassNoData
	}
	return VenueClassAdmin
}

// venueEdgeHosts is the allowlist: venue name -> hosts its edge may have.
// A venue that is not a key is refused. Production is never a key.
var venueEdgeHosts = map[string][]string{
	"bigboy-compose": {"localhost", "127.0.0.1", "::1", "bigboy"},
}

// Venues lists the venues that may carry a login token, sorted.
func Venues() []string {
	names := make([]string, 0, len(venueEdgeHosts))
	for name := range venueEdgeHosts {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// VenueStamp is what a receipt records about how its edge leg was
// authenticated, so an admin receipt is never mistaken for a viewer one.
type VenueStamp struct {
	Name      string `json:"name"`
	Role      string `json:"principal_role"`
	Superuser bool   `json:"principal_superuser"`
}

// CheckVenue is the fail-closed gate for a pre-obtained login token.
// Default (empty venue) refuses; production refuses; an unknown venue
// refuses; the acknowledgement must equal the venue; the edge host must
// be one the venue owns.
func CheckVenue(venue, ack, edgeURL string) error {
	switch {
	case venue == "":
		return errors.New("goapiproof: a login token for the edge leg needs -venue naming a non-production venue; the default is refusal")
	case strings.EqualFold(strings.TrimSpace(venue), VenueProduction):
		return errors.New("goapiproof: a login token is never accepted against production")
	}
	hosts, known := venueEdgeHosts[venue]
	if !known {
		return fmt.Errorf("goapiproof: venue %q is not an allowlisted venue %v", venue, Venues())
	}
	if ack != venue {
		return fmt.Errorf("goapiproof: %s must equal the venue name to accept a login token", VenueAckEnvVar)
	}
	parsed, err := url.Parse(edgeURL)
	if err != nil || parsed.Hostname() == "" {
		return errors.New("goapiproof: the edge URL has no host to check against the venue")
	}
	for _, host := range hosts {
		if strings.EqualFold(parsed.Hostname(), host) {
			return nil
		}
	}
	return fmt.Errorf("goapiproof: edge host %q is not a host of venue %q", parsed.Hostname(), venue)
}

// PeekPrincipal reads role and is_superuser from a JWT's payload WITHOUT
// verifying it: the edge verifies it on every request; this only labels
// the receipt.
func PeekPrincipal(token string) (role string, superuser bool, err error) {
	token = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(token), "Bearer "))
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return "", false, errors.New("goapiproof: the login token is not a three-segment JWT")
	}
	payload, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(parts[1], "="))
	if err != nil {
		return "", false, errors.New("goapiproof: the login token payload is not base64url")
	}
	var claims struct {
		Role      *string `json:"role"`
		Superuser *bool   `json:"is_superuser"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil {
		return "", false, errors.New("goapiproof: the login token payload is not JSON")
	}
	if claims.Role == nil || claims.Superuser == nil {
		return "", false, errors.New("goapiproof: the login token carries no role or is_superuser claim")
	}
	return *claims.Role, *claims.Superuser, nil
}

// AuthzRequirement is who may run an operation: any of Roles
// (case-insensitive), or a superuser.
type AuthzRequirement struct {
	Roles []string
}

// Satisfied reports whether a principal passes the requirement.
func (r AuthzRequirement) Satisfied(role string, superuser bool) bool {
	if superuser {
		return true
	}
	for _, allowed := range r.Roles {
		if strings.EqualFold(allowed, role) {
			return true
		}
	}
	return false
}

// dataHealthRequirement mirrors datahealth.RequireOperator; a test in that
// package pins the two together.
var dataHealthRequirement = AuthzRequirement{Roles: []string{"admin", "owner", "operator"}}

// operationAuthz names the operations whose Go route gates on more than a
// read role. An operation absent here is NEVER venue-eligible.
var operationAuthz = map[string]AuthzRequirement{
	"connectorsDataHealth":  dataHealthRequirement,
	"dataHealthIdentity":    dataHealthRequirement,
	"mappingCoverageHealth": dataHealthRequirement,
	"metricLineage":         dataHealthRequirement,
}

// AuthzFor returns the declared requirement for operation.
func AuthzFor(operation string) (AuthzRequirement, bool) {
	requirement, ok := operationAuthz[operation]
	return requirement, ok
}

// ViewerCanSatisfy reports whether ANY principal edgetokenmint can mint
// (its allowed roles, never a superuser) passes the requirement. Computed
// from the minter's own vocabulary, not from a list.
func ViewerCanSatisfy(requirement AuthzRequirement) bool {
	for role, allowed := range edgetokenmint.AllowedRoles {
		if allowed && requirement.Satisfied(role, false) {
			return true
		}
	}
	return false
}

// VenueEligible reports whether operation may be admitted from a venue
// receipt: it declares a requirement the viewer principal cannot meet.
func VenueEligible(operation string) bool {
	requirement, ok := AuthzFor(operation)
	return ok && !ViewerCanSatisfy(requirement)
}

// VenueEligibleOperations filters operations to the venue-eligible ones.
func VenueEligibleOperations(operations []string) []string {
	var eligible []string
	for _, operation := range operations {
		if VenueEligible(operation) {
			eligible = append(eligible, operation)
		}
	}
	sort.Strings(eligible)
	return eligible
}

// VenueOutcome is the subset of a report outcome venue admission reads.
type VenueOutcome struct {
	Operation        string   `json:"operation"`
	Variant          string   `json:"variant"`
	DocumentDigest   string   `json:"document_digest"`
	Executed         bool     `json:"executed"`
	Admitted         bool     `json:"admitted"`
	Route            string   `json:"route"`
	EdgeBuildBinding string   `json:"edge_build_binding"`
	TerminalState    string   `json:"terminal_state"`
	BaselineDefects  []string `json:"baseline_defect"`
	Outside          *int     `json:"differences_outside_baseline_defect"`
}

// VenueReceipt is the prover's JSON report as a venue run wrote it.
type VenueReceipt struct {
	SchemaDigest   string         `json:"schema_digest"`
	CandidateBuild string         `json:"candidate_build"`
	Stage          string         `json:"stage"`
	ExitCause      string         `json:"exit_cause"`
	Venue          *VenueStamp    `json:"venue"`
	Outcomes       []VenueOutcome `json:"outcomes"`

	// Digest is sha256 of the exact bytes parsed.
	Digest string `json:"-"`
}

// ParseVenueReceipt decodes a report; trailing bytes are refused.
func ParseVenueReceipt(raw []byte) (*VenueReceipt, error) {
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	var receipt VenueReceipt
	if err := decoder.Decode(&receipt); err != nil {
		return nil, fmt.Errorf("goapiproof: venue receipt: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, errors.New("goapiproof: venue receipt carries bytes after its JSON value")
	}
	sum := sha256.Sum256(raw)
	receipt.Digest = hex.EncodeToString(sum[:])
	return &receipt, nil
}

// venueExitCompleted is the prover's exit_cause for a whole run.
const venueExitCompleted = "completed"

// VenueAdmit decides whether receipt is enablement proof for operation at
// the build, schema and document production is running, for targetMode.
// It returns nil, or the first reason it is not.
func VenueAdmit(receipt *VenueReceipt, operation, schemaDigest, documentDigest, runningBuild, targetMode string) error {
	if receipt == nil {
		return errors.New("no venue receipt")
	}
	if !VenueEligible(operation) {
		return errors.New("operation is not venue-eligible: the viewer principal can prove it, or it declares no authorization requirement")
	}
	requirement, _ := AuthzFor(operation)
	stamp := receipt.Venue
	switch {
	case stamp == nil:
		return errors.New("receipt records no venue")
	case strings.EqualFold(strings.TrimSpace(stamp.Name), VenueProduction):
		return errors.New("receipt venue is production")
	case venueEdgeHosts[stamp.Name] == nil:
		return fmt.Errorf("receipt venue %q is not allowlisted", stamp.Name)
	case !requirement.Satisfied(stamp.Role, stamp.Superuser):
		return fmt.Errorf("receipt principal (role %q, superuser %t) does not satisfy the operation's authorization", stamp.Role, stamp.Superuser)
	case receipt.Stage != Stage:
		return fmt.Errorf("receipt stage %q is not %q", receipt.Stage, Stage)
	case receipt.ExitCause != venueExitCompleted:
		return fmt.Errorf("receipt exit_cause %q is not %q", receipt.ExitCause, venueExitCompleted)
	case strings.TrimSpace(runningBuild) == "" || receipt.CandidateBuild != runningBuild:
		return fmt.Errorf("receipt names build %q, production runs %q", receipt.CandidateBuild, runningBuild)
	case receipt.SchemaDigest != schemaDigest:
		return errors.New("receipt schema digest is not the live schema digest")
	case documentDigest == "":
		return errors.New("no live document digest for the operation")
	}
	spec, err := SpecFor(operation)
	if err != nil {
		return err
	}
	needed := map[string]bool{"": false}
	for _, variant := range spec.Variants {
		needed[variant.Name] = false
	}
	for _, outcome := range receipt.Outcomes {
		if outcome.Operation != operation {
			continue
		}
		if _, known := needed[outcome.Variant]; !known {
			return fmt.Errorf("receipt names variant %q the operation does not declare", outcome.Variant)
		}
		if reason := venueOutcomeRefusal(outcome, documentDigest, targetMode); reason != "" {
			return fmt.Errorf("receipt outcome (variant %q): %s", outcome.Variant, reason)
		}
		needed[outcome.Variant] = true
	}
	for name, seen := range needed {
		if !seen {
			return fmt.Errorf("receipt has no passing outcome for variant %q", name)
		}
	}
	return nil
}

// venueOutcomeRefusal mirrors enablementProofPredicate row by row.
func venueOutcomeRefusal(outcome VenueOutcome, documentDigest, targetMode string) string {
	switch {
	case outcome.DocumentDigest != documentDigest:
		return "document digest is not the live one"
	case !outcome.Executed || !outcome.Admitted:
		return "not executed and admitted"
	case outcome.EdgeBuildBinding != EdgeBuildPresent:
		return "edge build binding is not per_request"
	}
	switch targetMode {
	case TargetModePrimary:
		if outcome.Route != RouteEdge {
			return "route is not the edge route required for primary"
		}
	case TargetModeCanary:
		if outcome.Route == "" {
			return "route is not recorded"
		}
	default:
		return "unknown target mode"
	}
	switch outcome.TerminalState {
	case EnablementProofTerminalState:
		return ""
	case EnablementCitedMismatchState:
		if outcome.Outside == nil || *outcome.Outside != 0 || len(outcome.BaselineDefects) == 0 {
			return "mismatch is not fully cited"
		}
		for _, citation := range outcome.BaselineDefects {
			if strings.Trim(citation, blankCitationCutset) == "" {
				return "mismatch carries a blank citation"
			}
			if HasGoOnlyPrefix(citation) {
				return "mismatch carries the go-only citation"
			}
		}
		return ""
	}
	return fmt.Sprintf("terminal state %q is not enablement proof", outcome.TerminalState)
}
