package sessionissue

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/google/uuid"
)

// PathState is auth-service's session state route (acp ruling D6559 on
// CHAOS-12122): the session's owner, organization, effective revoke, effective
// expiry and token_version pin, as auth-service itself decides them.
const PathState = "/internal/v1/sessions/state"

// reasonSessionNotFound is the reason of the route's 404 for an unknown
// session. A 404 with any other reason, or none, is an auth-service without
// the route (an older build during a roll), never "no such session".
const reasonSessionNotFound = "session_not_found"

// ErrSessionNotFound is the route's answer for a session it does not hold.
// It is not ErrUnavailable: the session is known not to exist.
var ErrSessionNotFound = errors.New("sessionissue: auth-service holds no such session")

// State is one session's state.
type State struct {
	PrincipalID uuid.UUID
	// OrganizationID is nil for a session bound to no organization.
	OrganizationID *uuid.UUID
	// Revoked is the effective revoke: an explicit revoke, or a password
	// change after the session was created.
	Revoked bool
	// Expired is the effective expiry (the absolute cap or the idle expiry,
	// whichever is first) at or before auth-service's now.
	Expired bool
	// LegacyTokenVersion is the session's token_version pin; nil on a session
	// that has none.
	LegacyTokenVersion *int64
}

// StateReader reads one session's state.
type StateReader interface {
	State(ctx context.Context, sessionID uuid.UUID) (State, error)
}

var _ StateReader = (*Client)(nil)

// State asks auth-service for the state of sessionID, presenting the client's
// session issuer credential. A 404 with reason session_not_found is
// ErrSessionNotFound; every other failure (the route unreachable or slow, any
// other status, a 404 without that reason, or an answer that is not exactly
// the state document) wraps ErrUnavailable.
func (c *Client) State(ctx context.Context, sessionID uuid.UUID) (State, error) {
	raw, err := json.Marshal(map[string]string{"session_id": sessionID.String()})
	if err != nil {
		return State{}, fmt.Errorf("%w: encoding the state request: %w", ErrUnavailable, err)
	}
	out, err := http.NewRequestWithContext(ctx, http.MethodPost, c.stateEndpoint, bytes.NewReader(raw))
	if err != nil {
		return State{}, fmt.Errorf("%w: building the state request: %w", ErrUnavailable, err)
	}
	out.Header.Set("Authorization", "Bearer "+c.token)
	out.Header.Set("Content-Type", "application/json")
	out.Header.Set("Accept", "application/json")
	response, err := c.http.Do(out)
	if err != nil {
		return State{}, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	defer response.Body.Close()
	answer, err := io.ReadAll(io.LimitReader(response.Body, maxAnswerBytes+1))
	if err != nil {
		return State{}, fmt.Errorf("%w: reading the state answer: %w", ErrUnavailable, err)
	}
	if response.StatusCode != http.StatusOK {
		refused := refusalIn(response.StatusCode, answer)
		if refused != nil && refused.Status == http.StatusNotFound && refused.Reason == reasonSessionNotFound {
			return State{}, ErrSessionNotFound
		}
		if refused != nil {
			return State{}, refused
		}
		return State{}, fmt.Errorf("%w: state route status %d", ErrUnavailable, response.StatusCode)
	}
	if len(answer) > maxAnswerBytes {
		return State{}, fmt.Errorf("%w: state answer over %d bytes", ErrUnavailable, maxAnswerBytes)
	}
	state, err := parseState(answer)
	if err != nil {
		return State{}, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	return state, nil
}

var errStateAnswer = errors.New("the state answer is not exactly the session state document")

// parseState reads exactly the five members of the state document: principal_id
// (a canonical UUID), organization_id (a canonical UUID or null), revoked and
// expired (booleans) and legacy_token_version (an integer or null). A missing,
// repeated or unknown member is refused.
func parseState(answer []byte) (State, error) {
	dec := json.NewDecoder(bytes.NewReader(answer))
	if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
		return State{}, errStateAnswer
	}
	members := map[string]json.RawMessage{}
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return State{}, errStateAnswer
		}
		name, _ := tok.(string)
		if _, seen := members[name]; seen {
			return State{}, errStateAnswer
		}
		var value json.RawMessage
		if err := dec.Decode(&value); err != nil {
			return State{}, errStateAnswer
		}
		members[name] = value
	}
	if tok, err := dec.Token(); err != nil || tok != json.Delim('}') {
		return State{}, errStateAnswer
	}
	if _, err := dec.Token(); err != io.EOF {
		return State{}, errStateAnswer
	}
	if len(members) != 5 {
		return State{}, errStateAnswer
	}
	var out State
	principal, err := canonicalUUID(members["principal_id"])
	if err != nil {
		return State{}, err
	}
	out.PrincipalID = principal
	switch org := members["organization_id"]; {
	case org == nil:
		return State{}, errStateAnswer
	case string(org) != "null":
		id, err := canonicalUUID(org)
		if err != nil {
			return State{}, err
		}
		out.OrganizationID = &id
	}
	if err := strictBool(members["revoked"], &out.Revoked); err != nil {
		return State{}, err
	}
	if err := strictBool(members["expired"], &out.Expired); err != nil {
		return State{}, err
	}
	switch pin := members["legacy_token_version"]; {
	case pin == nil:
		return State{}, errStateAnswer
	case string(pin) != "null":
		if pin[0] == '"' {
			return State{}, errStateAnswer
		}
		var version int64
		dec := json.NewDecoder(bytes.NewReader(pin))
		dec.UseNumber()
		var number json.Number
		if err := dec.Decode(&number); err != nil {
			return State{}, errStateAnswer
		}
		if version, err = number.Int64(); err != nil {
			return State{}, errStateAnswer
		}
		out.LegacyTokenVersion = &version
	}
	return out, nil
}

func canonicalUUID(raw json.RawMessage) (uuid.UUID, error) {
	var text string
	if raw == nil || json.Unmarshal(raw, &text) != nil {
		return uuid.UUID{}, errStateAnswer
	}
	id, err := uuid.Parse(text)
	if err != nil || id.String() != text {
		return uuid.UUID{}, errStateAnswer
	}
	return id, nil
}

func strictBool(raw json.RawMessage, into *bool) error {
	switch string(raw) {
	case "true":
		*into = true
	case "false":
		*into = false
	default:
		return errStateAnswer
	}
	return nil
}
