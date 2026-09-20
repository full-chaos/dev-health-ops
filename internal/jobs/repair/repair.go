// Package repair advances the ledger rows an operator must resolve by hand:
// a work-graph execution request left ambiguous, a metric compatibility
// execution left ambiguous or stuck executing, and the bulk form of the
// latter for the daily runs an operator has already scoped for redrive.
//
// Each operation is one Postgres transaction that locks the ledger row,
// re-checks the operator's expectations against it (state, attempt count, no
// live claim), moves it to its resolved state and records the repair. Nothing
// is computed and no other system is called: the repair only decides which
// state a row that cannot resolve on its own is allowed to move to.
package repair

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

const (
	// ReviewEvidenceMaxBytes bounds the operator's statement of what they
	// verified, in UTF-8 bytes of the text as submitted.
	ReviewEvidenceMaxBytes = 2048
	// OutputEvidenceMaxBytes bounds the canonical encoding of the output
	// evidence a confirm_succeeded resolution records.
	OutputEvidenceMaxBytes = 4096

	ResolutionRetrySafe        = "retry_safe"
	ResolutionConfirmSucceeded = "confirm_succeeded"
)

// DB opens the transactions a repair runs in; *pgxpool.Pool satisfies it.
type DB interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}

// Refusal is a repair the ledger's state or the request's content forbids.
// Status follows the HTTP class of the refusal (404, 409, 422) so operators
// see the same reason text they always did.
type Refusal struct {
	Status int
	Detail string
}

func (r *Refusal) Error() string { return fmt.Sprintf("repair refused (%d): %s", r.Status, r.Detail) }

func refuse(status int, detail string) error { return &Refusal{Status: status, Detail: detail} }

// AsRefusal reports whether err is a Refusal.
func AsRefusal(err error) (*Refusal, bool) {
	var r *Refusal
	if errors.As(err, &r) {
		return r, true
	}
	return nil, false
}

// canonicalJSON encodes a decoded JSON value the way Python's
// json.dumps(value, sort_keys=True, separators=(",", ":")) does: keys sorted,
// no whitespace, non-ASCII escaped, floats in their shortest repr. Numbers
// arrive as json.Number (the decoder is told to keep the operator's digits);
// an integer literal stays an integer and anything with a fraction or exponent
// is a float, as Python's decoder types it. An integer beyond 64 bits is
// refused: the encoder here does not carry arbitrary-precision integers.
func canonicalJSON(value any) (string, error) {
	converted, err := convertNumbers(value)
	if err != nil {
		return "", err
	}
	encoded, err := pythonparity.MarshalPythonJSONCompact(converted)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

func convertNumbers(value any) (any, error) {
	switch typed := value.(type) {
	case json.Number:
		text := typed.String()
		if !strings.ContainsAny(text, ".eE") {
			n, err := strconv.ParseInt(text, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("integer %s does not fit in 64 bits", text)
			}
			return n, nil
		}
		f, err := strconv.ParseFloat(text, 64)
		if err != nil {
			return nil, fmt.Errorf("number %s is not a finite float", text)
		}
		return f, nil
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, item := range typed {
			converted, err := convertNumbers(item)
			if err != nil {
				return nil, err
			}
			out[key] = converted
		}
		return out, nil
	case []any:
		out := make([]any, len(typed))
		for index, item := range typed {
			converted, err := convertNumbers(item)
			if err != nil {
				return nil, err
			}
			out[index] = converted
		}
		return out, nil
	default:
		return value, nil
	}
}

// decodeJSONB reads a jsonb value the way Python's driver returns it: numbers
// keep their integer or float type.
func decodeJSONB(raw []byte) (any, error) {
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	return value, nil
}

// validReviewEvidence is the bound the ledger's own column enforces: at least
// one character and at most ReviewEvidenceMaxBytes bytes as submitted.
func validReviewEvidence(text string) bool {
	return text != "" && len(text) <= ReviewEvidenceMaxBytes
}

func validResolution(resolution string) bool {
	return resolution == ResolutionRetrySafe || resolution == ResolutionConfirmSucceeded
}

func isNoRows(err error) bool { return errors.Is(err, pgx.ErrNoRows) }
