package goapiproof

import "strings"

// DuplicateCopyRule states which fields two physical copies of one
// logical row may disagree on, for a baseline plane that returns every
// unmerged physical version of a ReplacingMergeTree row while the
// candidate plane reads the table FINAL and returns the one version the
// merge keeps. The rule is a property of the table's writer, never of a
// captured body: an entry names a field only when the writer can store a
// different value for it on a later write of the same key.
//
// A FINAL read returns one whole physical version, never a mix of
// fields from several versions, and an unFINALed read of the same table
// sees every version still unmerged, the kept one included. So the
// candidate row must equal one baseline copy field for field. Which copy
// the merge keeps is the newest by the table's version column, and no
// response body carries that column; the rule reads the newest copy's
// direction off the write-once fields only (a later write never turns a
// populated value back to null). A candidate that equals an older copy
// on every write-once field and differs only in a rewritten field is
// not told apart from one equal to the newest copy.
type DuplicateCopyRule struct {
	// WriteOnceFields change only from null to one populated value on a
	// later write of the same key: across the copies, at most one
	// populated value, and the candidate carries it whenever any copy
	// does. A field derived from one of them belongs here with it.
	WriteOnceFields []string
	// RewrittenFields take whatever value the upstream source holds at
	// each write, in no fixed direction: the copies may carry different
	// values, and the candidate carries the value of the copy it equals.
	RewrittenFields []string
}

// declared reports whether the rule names any field. A nil or empty rule
// admits only copies equal to each other and to the candidate.
func (r *DuplicateCopyRule) declared() bool {
	return r != nil && (len(r.WriteOnceFields) > 0 || len(r.RewrittenFields) > 0)
}

// describe names the rule's field sets for a refusal detail.
func (r *DuplicateCopyRule) describe() string {
	if !r.declared() {
		return "duplicate-row"
	}
	parts := []string{}
	if len(r.WriteOnceFields) > 0 {
		parts = append(parts, "write-once "+strings.Join(r.WriteOnceFields, "/"))
	}
	if len(r.RewrittenFields) > 0 {
		parts = append(parts, "rewritten "+strings.Join(r.RewrittenFields, "/"))
	}
	return strings.Join(parts, " and ")
}

// candidateIsServedCopy reports whether candidate is a copy a FINAL read
// of group's own key can return under this rule: every copy carries the
// candidate's key set; the candidate equals one copy field for field
// (jsonValuesEqual); every field outside the rule agrees across the
// copies; each write-once field carries at most one populated value
// across the copies, and the candidate carries it when any copy does.
func (r *DuplicateCopyRule) candidateIsServedCopy(group []map[string]any, candidate map[string]any) bool {
	if len(group) == 0 || candidate == nil {
		return false
	}
	for _, copyObject := range group {
		if !sameKeySet(copyObject, candidate) {
			return false
		}
	}
	equalsACopy := false
	for _, copyObject := range group {
		if jsonValuesEqual(copyObject, candidate) {
			equalsACopy = true
			break
		}
	}
	if !equalsACopy {
		return false
	}
	var writeOnce, rewritten []string
	if r != nil {
		rewritten = r.RewrittenFields
		// Every copy carries the candidate's key set (checked above), so
		// a write-once field the candidate lacks is absent from every
		// copy too and has nothing to reconcile.
		for _, field := range r.WriteOnceFields {
			if _, present := candidate[field]; present {
				writeOnce = append(writeOnce, field)
			}
		}
	}
	stripped := make([]map[string]any, len(group))
	for i, copyObject := range group {
		stripped[i] = withoutFields(copyObject, rewritten)
	}
	representative, ok := writeOnceRepresentative(stripped, writeOnce)
	return ok && jsonValuesEqual(representative, withoutFields(candidate, rewritten))
}

// sameKeySet reports whether a and b carry exactly the same field names.
func sameKeySet(a, b map[string]any) bool {
	if len(a) != len(b) {
		return false
	}
	if len(a) == 0 {
		return true
	}
	for key := range a {
		if _, present := b[key]; !present {
			return false
		}
	}
	return true
}

// withoutFields returns a shallow copy of object without the named
// fields.
func withoutFields(object map[string]any, fields []string) map[string]any {
	drop := make(map[string]bool, len(fields))
	for _, field := range fields {
		drop[field] = true
	}
	out := make(map[string]any, len(object))
	if len(object) == 0 {
		return out
	}
	for key, value := range object {
		if !drop[key] {
			out[key] = value
		}
	}
	return out
}
