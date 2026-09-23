package goapiproof

import "fmt"

// This file backs RESTBodyModeCandidateShape (restcorpus.go): for a
// request whose baseline never carries comparable evidence (see
// restdeletedbody.go), the candidate's OWN decoded body is judged
// against its declared JSON kind instead of against a second leg.
// "Shape" here is deliberately narrow -- root KIND (object vs array) plus
// non-emptiness for an object -- not a field-by-field schema: the corpus
// already knows each route's kind from its Python response_model (a
// list[...] or a BaseModel), and asserting more than that would mean
// hand-declaring every field name per route with no second leg to catch
// a wrong one, which risks a wrong assertion failing every future proof
// run rather than catching a real regression.

// RESTRefusalCandidateShapeInvalid is the named refusal reason for a
// RESTBodyModeCandidateShape request whose candidate body decoded but did
// not pass AssertRESTCandidateShape -- a null root, the wrong JSON kind
// for CandidateShapeArray, or an empty object where a real answer must
// carry at least one field.
const RESTRefusalCandidateShapeInvalid = "rest_candidate_body_did_not_carry_a_live_shape"

// AssertRESTCandidateShape reports whether data -- a decoded candidate
// body (typically Snapshot.Data) -- is "live" for arrayShaped's declared
// kind:
//
//   - nil (a bare JSON `null`) is never live, either kind: a real answer
//     is never absent entirely.
//   - arrayShaped true: data must be a JSON array ([]any). An empty array
//     passes -- a scope with genuinely zero rows is still a live answer,
//     not a missing one.
//   - arrayShaped false: data must be a JSON object (map[string]any) AND
//     non-empty. Every object-shaped response_model this corpus proves
//     carries multiple always-present fields; an empty object is never a
//     real answer for one of them, so it fails liveness rather than
//     passing as a technically-valid empty map.
//
// Returns a non-nil error naming exactly which of these failed; nil means
// live.
func AssertRESTCandidateShape(data any, arrayShaped bool) error {
	if data == nil {
		return fmt.Errorf("goapiproof: candidate body decoded to a null root")
	}
	if arrayShaped {
		if _, ok := data.([]any); !ok {
			return fmt.Errorf("goapiproof: candidate body root is not a JSON array (declared array-shaped)")
		}
		return nil
	}
	obj, ok := data.(map[string]any)
	if !ok {
		return fmt.Errorf("goapiproof: candidate body root is not a JSON object (declared object-shaped)")
	}
	if len(obj) == 0 {
		return fmt.Errorf("goapiproof: candidate body root is an empty JSON object")
	}
	return nil
}
