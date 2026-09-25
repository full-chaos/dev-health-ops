package writeproof

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
)

// Terminal states a write proof can reach once the mutation was posted and its
// effects read (the receipt's terminal_state vocabulary).
const (
	StateMatch       = "match"
	StateMismatch    = "mismatch"
	StateProofFailed = "proof_failed"
)

// Option adjusts one Execute call.
type Option func(*settings)

type settings struct{ requiredBuild string }

// WithRequiredBuild requires the mutation's response to carry exactly this build
// in its x-dev-health-build header (the build of the process that served it). A
// response without it can never be a match: the run is proof_failed and the
// dataset is KEPT, because nothing has tied the write to the candidate build.
func WithRequiredBuild(build string) Option {
	return func(s *settings) { s.requiredBuild = build }
}

// Response is what the posted mutation answered.
type Response struct {
	Status int
	Body   []byte
	// Build is the x-dev-health-build header of THIS response (the build of the
	// process that served it), or empty when it carried none.
	Build string
	// WireAttempts is how many connections the transport used for the one post
	// (goapiproof.LegResponse.WireAttempts). More than one means the write may
	// have been sent twice.
	WireAttempts int
}

// Poster sends the mutation document with the case's variables VERBATIM and
// returns the answer. Execute calls it exactly once; a Poster must not retry.
type Poster func(ctx context.Context, document, variablesJSON string) (Response, error)

// ErrNotExecuted marks a failure BEFORE the mutation was posted (a refused case,
// a seed that failed): nothing was written by the mutation and no receipt is
// owed. A failure after the post is never an error but a Result, because the
// effects must be kept and recorded.
var ErrNotExecuted = errors.New("writeproof: the mutation was not executed")

// Forensics names a dataset a failed run left in place.
type Forensics struct {
	Org string
	Run RunTag
}

// Result is one Execute call's outcome.
type Result struct {
	Case           string
	Operation      string
	TerminalState  string
	Digest         string
	BaselineDigest string
	Detail         string
	// ServedBuild is the build header the mutation's response carried.
	ServedBuild string
	// Posts is how many times the mutation was posted. It is 1 for every Result:
	// a second post would be a second write.
	Posts int
	// Kept is set when the dataset was left in place (any outcome but a match).
	Kept *Forensics
	// TeardownErr is a failed cleanup after a match; the receipt stands.
	TeardownErr error
	Effects     Effects
}

// Execute proves one case: seed inside org, post the mutation once, read and
// normalize what was persisted, digest it, compare with the baseline.
//
// document is the mutation's registered text (from the running build's
// registry, verified by the caller). A dataset is removed only after a match;
// after anything else it is kept and named in Result.Kept.
func Execute(ctx context.Context, db goapiproof.Querier, org string, c Case, run RunTag, document string, post Poster, options ...Option) (Result, error) {
	var settings settings
	for _, option := range options {
		option(&settings)
	}
	if err := c.Validate(); err != nil {
		return Result{}, fmt.Errorf("%w: %w", ErrNotExecuted, err)
	}
	if strings.TrimSpace(org) == "" || run == "" || strings.TrimSpace(document) == "" || post == nil {
		return Result{}, fmt.Errorf("%w: an org, a run tag, the registered document and a poster are all required", ErrNotExecuted)
	}
	if !json.Valid([]byte(c.VariablesJSON)) {
		return Result{}, fmt.Errorf("%w: case %q variables are not valid JSON", ErrNotExecuted, c.Name)
	}

	result := Result{Case: c.Name, Operation: c.Operation, BaselineDigest: c.BaselineDigest}
	if err := c.Seeder.Seed(ctx, db, org, run); err != nil {
		// A partial seed may exist: keep it named, the next run's cleanup or a
		// human removes it.
		return Result{Kept: &Forensics{Org: org, Run: run}}, fmt.Errorf("%w: seed case %q: %w (partial dataset kept under run tag %s)", ErrNotExecuted, c.Name, err, run)
	}
	start := time.Now().UTC()

	// THE post. One call, no retry, whatever it returns.
	result.Posts = 1
	response, postErr := post(ctx, document, c.VariablesJSON)
	normalizer := NewNormalizer(c.KeepIDs, run, start)

	effects := Effects{Case: c.Name, Tables: map[string][]any{}}
	var detail []string
	switch {
	case postErr != nil:
		// The write MAY have run: read what was persisted anyway, so the record
		// says what is there instead of guessing.
		detail = append(detail, "the post failed ("+postErr.Error()+"): the mutation may have run, its effects are read and kept")
		effects.Response = "<no response>"
	default:
		result.ServedBuild = response.Build
		parsed, problem := parseResponse(response)
		if problem != "" {
			detail = append(detail, problem)
		}
		if settings.requiredBuild != "" && response.Build != settings.requiredBuild {
			// Which build wrote is not established by this response: whatever it
			// persisted is no evidence for the candidate build, and the dataset is
			// the only remaining evidence of what ran, so it is kept.
			detail = append(detail, fmt.Sprintf("the mutation's response does not carry the candidate build in x-dev-health-build (got %q): which build performed the write is not established", response.Build))
		}
		if response.WireAttempts > 1 {
			detail = append(detail, fmt.Sprintf("the transport used %d connections for one post: the mutation may have been sent more than once", response.WireAttempts))
		}
		normalized, err := normalizer.Value(parsed)
		if err != nil {
			return result, fmt.Errorf("writeproof: normalize the response of case %q: %w (dataset kept under run tag %s)", c.Name, err, run)
		}
		effects.Response = normalized
	}

	for _, table := range c.Tables {
		rows, err := readTable(ctx, db, table, org, run, normalizer)
		if err != nil {
			return result, fmt.Errorf("writeproof: read table %q of case %q: %w (dataset kept under run tag %s)", table.Label, c.Name, err, run)
		}
		effects.Tables[table.Label] = rows
	}
	result.Effects = effects
	digest, err := effects.Digest()
	if err != nil {
		return result, err
	}
	result.Digest = digest

	switch {
	case effects.Rows() == 0:
		// A digest over nothing matches any other nothing: the mutation did not
		// persist what the case says it persists (or the comparison reads the
		// wrong rows). Never a match.
		result.TerminalState = StateProofFailed
		detail = append(detail, "no comparison table returned a row: nothing was observed to compare")
	case postErr != nil || len(detail) > 0:
		result.TerminalState = StateProofFailed
	case digest == c.BaselineDigest:
		result.TerminalState = StateMatch
	default:
		result.TerminalState = StateMismatch
		detail = append(detail, "the persisted effects differ from the committed baseline digest")
	}
	result.Detail = strings.Join(detail, "; ")

	if result.TerminalState == StateMatch {
		if err := c.Seeder.Teardown(ctx, db, org, run); err != nil {
			result.TeardownErr = err
			result.Kept = &Forensics{Org: org, Run: run}
		}
		return result, nil
	}
	result.Kept = &Forensics{Org: org, Run: run}
	return result, nil
}

// parseResponse decodes the GraphQL answer (numbers kept exact) and names why it
// cannot count as a successful execution: a non-200 status, a body that is not a
// JSON object, or a non-empty errors member.
func parseResponse(response Response) (any, string) {
	var problems []string
	if response.Status != 200 {
		problems = append(problems, fmt.Sprintf("the mutation answered HTTP %d", response.Status))
	}
	decoder := json.NewDecoder(bytes.NewReader(response.Body))
	decoder.UseNumber()
	var parsed any
	if err := decoder.Decode(&parsed); err != nil || decoder.More() {
		problems = append(problems, "the answer is not one JSON document")
		return "<unparseable response>", strings.Join(problems, "; ")
	}
	object, ok := parsed.(map[string]any)
	if !ok {
		problems = append(problems, "the answer is not a JSON object")
	} else if errs, present := object["errors"]; present {
		if list, isList := errs.([]any); !isList || len(list) > 0 {
			problems = append(problems, "the answer carries a non-empty errors member")
		}
	}
	return parsed, strings.Join(problems, "; ")
}

func readTable(ctx context.Context, db goapiproof.Querier, table Table, org string, run RunTag, normalizer *Normalizer) ([]any, error) {
	rows, err := db.Query(ctx, table.SQL, org, string(run))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	columns := rows.FieldDescriptions()
	out := []any{}
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, err
		}
		row := make(map[string]any, len(values))
		for i, value := range values {
			row[string(columns[i].Name)] = value
		}
		normalized, err := normalizer.Value(row)
		if err != nil {
			return nil, err
		}
		out = append(out, normalized)
	}
	return out, rows.Err()
}
