package jobcontract

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"unicode/utf8"
)

const maxJSONDepth = 16

// decodeLenient applies every guard decodeStrict applies -- size, UTF-8,
// structure, trailing data -- but does NOT reject unknown fields.
//
// It exists for exactly one caller: loading a MERGE BASE contract tree for
// comparison. An older tree legitimately carries fields the current struct has
// dropped, and rejecting them is not a safety property, it is an inability to
// read history (CHAOS-3903 follow-up).
func decodeLenient(data []byte, maxBytes int, destination any) error {
	return decodeJSON(data, maxBytes, destination, false, nil)
}

func decodeStrict(data []byte, maxBytes int, destination any) error {
	return decodeJSON(data, maxBytes, destination, true, nil)
}

func decodeJSON(data []byte, maxBytes int, destination any, disallowUnknownFields bool, guard *valueGuard) error {
	if len(data) == 0 {
		return errors.New("JSON value is empty")
	}
	if len(data) > maxBytes {
		return fmt.Errorf("JSON value exceeds %d bytes", maxBytes)
	}
	if !utf8.Valid(data) {
		return errors.New("JSON must be UTF-8")
	}
	if err := validateJSONTokens(data, guard); err != nil {
		if refusal, ok := asValueRefusal(err); ok {
			return refusal
		}
		return errors.New("invalid JSON structure")
	}

	decoder := json.NewDecoder(bytes.NewReader(data))
	if disallowUnknownFields {
		decoder.DisallowUnknownFields()
	}
	if err := decoder.Decode(destination); err != nil {
		return errors.New("JSON does not match contract")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("JSON has trailing data")
	}
	return nil
}

func validateJSONTokens(data []byte, guard *valueGuard) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := consumeJSONValue(decoder, 0, guard); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return fmt.Errorf("trailing JSON: %w", err)
	}
	return nil
}

func consumeJSONValue(decoder *json.Decoder, depth int, guard *valueGuard) error {
	if depth > maxJSONDepth {
		return fmt.Errorf("JSON nesting exceeds %d levels", maxJSONDepth)
	}
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		if text, isString := token.(string); isString && guard != nil {
			return guard.checkString(depth, text)
		}
		return nil
	}

	switch delimiter {
	case '{':
		keys := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return errors.New("object key is not a string")
			}
			if guard != nil {
				if err := guard.checkKey(depth, key); err != nil {
					return err
				}
			}
			if _, exists := keys[key]; exists {
				return errors.New("duplicate JSON key")
			}
			keys[key] = struct{}{}
			if err := consumeJSONValue(decoder, depth+1, guard); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil {
			return err
		}
		if closing != json.Delim('}') {
			return errors.New("object has invalid closing delimiter")
		}
	case '[':
		for decoder.More() {
			if guard != nil {
				guard.keys[depth+1] = arrayElementMarker
			}
			if err := consumeJSONValue(decoder, depth+1, guard); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil {
			return err
		}
		if closing != json.Delim(']') {
			return errors.New("array has invalid closing delimiter")
		}
	default:
		return fmt.Errorf("unexpected delimiter %q", delimiter)
	}
	return nil
}
