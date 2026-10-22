package teamsidentity

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// linearGraphQLErrorEntry is one element of a GraphQL "errors" array.
type linearGraphQLErrorEntry struct {
	Message    string `json:"message"`
	Extensions struct {
		Code string `json:"code"`
		Type string `json:"type"`
	} `json:"extensions"`
}

// linearNotFound reports whether Linear names the entity as missing.
func (e linearGraphQLErrorEntry) linearNotFound() bool {
	kind := strings.ToLower(e.Extensions.Type)
	return strings.EqualFold(e.Extensions.Code, "NOT_FOUND") ||
		strings.Contains(kind, "not found") || strings.Contains(kind, "not_found") ||
		strings.HasPrefix(strings.ToLower(strings.TrimSpace(e.Message)), "entity not found")
}

// errLinearGraphQLNotFound is what a Linear "not found" becomes: the class the
// shared provider helper maps to a 404. Its text names no upstream message.
func errLinearGraphQLNotFound() error {
	return &providerfoundation.ProviderError{Class: providerfoundation.ErrorNotFound, Reason: "graphql_not_found"}
}

// linearErrorsFailure turns the GraphQL errors of a response into the error the
// route answers: a not-found marker is a not_found ProviderError, anything else
// keeps the generic error (a 500).
func linearErrorsFailure(entries []linearGraphQLErrorEntry, generic string) error {
	for _, entry := range entries {
		if entry.linearNotFound() {
			return errLinearGraphQLNotFound()
		}
	}
	return errors.New(generic)
}

// linearErrorsFromBody reads the "errors" array of a GraphQL body, nil when the
// body is not one.
func linearErrorsFromBody(raw []byte) []linearGraphQLErrorEntry {
	var payload struct {
		Errors []linearGraphQLErrorEntry `json:"errors"`
	}
	if json.Unmarshal(raw, &payload) != nil {
		return nil
	}
	return payload.Errors
}

// linearTransportFailure reclassifies a non-2xx Linear answer whose body is a
// GraphQL not-found: Linear reports GraphQL errors on a 4xx the client class
// as permanent. Any other error is returned unchanged.
func linearTransportFailure(err error) error {
	var providerErr *providerfoundation.ProviderError
	if err == nil || !errors.As(err, &providerErr) || providerErr.Class == providerfoundation.ErrorNotFound {
		return err
	}
	for _, entry := range linearErrorsFromBody([]byte(providerErr.Body)) {
		if entry.linearNotFound() {
			return errLinearGraphQLNotFound()
		}
	}
	return err
}

func linearGenericError(entries []linearGraphQLErrorEntry) string {
	if len(entries) == 0 {
		return "linear graphql error"
	}
	return fmt.Sprintf("linear graphql error: %s", entries[0].Message)
}
