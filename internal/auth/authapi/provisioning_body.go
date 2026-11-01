package authapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strconv"

	"github.com/google/uuid"
)

// maxProvisioningBody bounds every provisioning request.
const maxProvisioningBody = 4 << 10

// Field bounds inside maxProvisioningBody. description and request_metadata
// are go-api's audit_logs request part: carried to that row, never logged.
const (
	maxDescriptionBytes     = 512
	maxRequestMetadataBytes = 2048
	maxShortTextBytes       = 320
	maxTokenBytes           = 256
)

var errProvisioningBody = errors.New("authapi: not a provisioning request")

// strictObject reads one JSON object: no duplicate member, nothing after it,
// and only the names allowed lists (each optional unless the field reader
// requires it).
type strictObject map[string]json.RawMessage

func readStrictObject(body io.Reader, allowed ...string) (strictObject, error) {
	dec := json.NewDecoder(body)
	dec.UseNumber()
	if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
		return nil, errProvisioningBody
	}
	known := map[string]bool{}
	for _, name := range allowed {
		known[name] = true
	}
	members := strictObject{}
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return nil, errProvisioningBody
		}
		name, _ := tok.(string)
		if _, seen := members[name]; seen || !known[name] {
			return nil, errProvisioningBody
		}
		var value json.RawMessage
		if err := dec.Decode(&value); err != nil {
			return nil, errProvisioningBody
		}
		members[name] = value
	}
	if tok, err := dec.Token(); err != nil || tok != json.Delim('}') {
		return nil, errProvisioningBody
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, errProvisioningBody
	}
	return members, nil
}

func (o strictObject) isNull(name string) bool {
	raw, ok := o[name]
	return !ok || string(raw) == "null"
}

// uuid reads a required canonical (lowercase, hyphenated) UUID.
func (o strictObject) uuid(name string) (uuid.UUID, error) {
	var text string
	if err := json.Unmarshal(o[name], &text); err != nil {
		return uuid.UUID{}, errProvisioningBody
	}
	id, err := uuid.Parse(text)
	if err != nil || text != id.String() {
		return uuid.UUID{}, errProvisioningBody
	}
	return id, nil
}

// optionalUUID reads an absent or null member as nil.
func (o strictObject) optionalUUID(name string) (*uuid.UUID, error) {
	if o.isNull(name) {
		return nil, nil
	}
	id, err := o.uuid(name)
	if err != nil {
		return nil, err
	}
	return &id, nil
}

// text reads a required string of 1 to max bytes.
func (o strictObject) text(name string, max int) (string, error) {
	var text string
	if err := json.Unmarshal(o[name], &text); err != nil || text == "" || len(text) > max {
		return "", errProvisioningBody
	}
	return text, nil
}

// optionalText reads an absent or null member as nil, else text.
func (o strictObject) optionalText(name string, max int) (*string, error) {
	if o.isNull(name) {
		return nil, nil
	}
	text, err := o.text(name, max)
	if err != nil {
		return nil, err
	}
	return &text, nil
}

// anyText reads a required string of 0 to max bytes.
func (o strictObject) anyText(name string, max int) (string, error) {
	if o.isNull(name) {
		return "", errProvisioningBody
	}
	var text string
	if err := json.Unmarshal(o[name], &text); err != nil || len(text) > max {
		return "", errProvisioningBody
	}
	return text, nil
}

// optionalAnyText reads an absent or null member as nil, else anyText.
func (o strictObject) optionalAnyText(name string, max int) (*string, error) {
	if o.isNull(name) {
		return nil, nil
	}
	text, err := o.anyText(name, max)
	if err != nil {
		return nil, err
	}
	return &text, nil
}

// optionalInt reads an absent or null member as nil, else an integer token
// (digits only: no fraction, no exponent) in [min, max].
func (o strictObject) optionalInt(name string, min, max int64) (*int64, error) {
	if o.isNull(name) {
		return nil, nil
	}
	raw := o[name]
	for _, c := range raw {
		if c < '0' || c > '9' {
			return nil, errProvisioningBody
		}
	}
	n, err := strconv.ParseInt(string(raw), 10, 64)
	if err != nil || n < min || n > max {
		return nil, errProvisioningBody
	}
	return &n, nil
}

// optionalObject reads an absent or null member as nil, else a JSON object of
// at most max bytes, compacted.
func (o strictObject) optionalObject(name string, max int) ([]byte, error) {
	if o.isNull(name) {
		return nil, nil
	}
	raw := bytes.TrimSpace(o[name])
	if len(raw) == 0 || raw[0] != '{' || len(raw) > max {
		return nil, errProvisioningBody
	}
	if !uniqueMembers(json.NewDecoder(bytes.NewReader(raw))) {
		return nil, errProvisioningBody
	}
	var out bytes.Buffer
	if err := json.Compact(&out, raw); err != nil {
		return nil, errProvisioningBody
	}
	return out.Bytes(), nil
}

// uniqueMembers reports whether the one JSON value dec holds is well formed
// with no object holding a member name twice, at any depth.
func uniqueMembers(dec *json.Decoder) bool {
	var value func() bool
	value = func() bool {
		tok, err := dec.Token()
		if err != nil {
			return false
		}
		switch tok {
		case json.Delim('{'):
			seen := map[string]bool{}
			for dec.More() {
				name, err := dec.Token()
				key, ok := name.(string)
				if err != nil || !ok || seen[key] {
					return false
				}
				seen[key] = true
				if !value() {
					return false
				}
			}
			end, err := dec.Token()
			return err == nil && end == json.Delim('}')
		case json.Delim('['):
			for dec.More() {
				if !value() {
					return false
				}
			}
			end, err := dec.Token()
			return err == nil && end == json.Delim(']')
		}
		return true
	}
	if !value() {
		return false
	}
	_, err := dec.Token()
	return err == io.EOF
}
