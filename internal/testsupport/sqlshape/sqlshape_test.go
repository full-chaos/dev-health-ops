package sqlshape

import "testing"

// TestPreservesPriorResultCategoryMeasuresMergeDirection is the detector's own
// test. The cases that matter most are the NESTED ones: the regexp this
// replaced spanned jsonb_build_object's arguments with `[^)]*`, so any stamp
// carrying a `to_jsonb(...)` argument -- which every lease-repair stamp does --
// slipped past it in the preserving direction.
func TestPreservesPriorResultCategoryMeasuresMergeDirection(t *testing.T) {
	for _, test := range []struct {
		name string
		sql  string
		want bool
	}{
		{
			name: "reads the prior category back out",
			sql:  `SET result = jsonb_build_object('error_category', COALESCE(unit.result->>'error_category', 'worker_lost'))`,
			want: true,
		},
		{
			name: "reads the prior category back out with the -> operator",
			sql:  `SET result = jsonb_build_object('error_category', unit.result -> 'error_category')`,
			want: true,
		},
		{
			name: "prior document on the right of a flat merge",
			sql:  `SET result = jsonb_build_object('error_category', 'worker_lost') || COALESCE(unit.result::jsonb, '{}'::jsonb)`,
			want: true,
		},
		{
			name: "prior document on the right of a merge whose build_object nests calls",
			sql: `SET result = (
				jsonb_build_object(
					'error_category', 'worker_lost',
					'next_retry_at', to_jsonb($4::timestamptz),
					'retry_surfaces', to_jsonb($5::text[])
				) || COALESCE(unit.result::jsonb, '{}'::jsonb)
			)`,
			want: true,
		},
		{
			name: "bare prior document on the right",
			sql:  `SET result = jsonb_build_object('error_category', 'worker_lost') || unit.result`,
			want: true,
		},
		{
			name: "prior document on the right of the SECOND operator in a chain",
			sql: `SET result = (
				COALESCE(unit.result::jsonb, '{}'::jsonb) ||
				jsonb_build_object('error_category', 'worker_lost') ||
				COALESCE(unit.result::jsonb, '{}'::jsonb)
			)`,
			want: true,
		},
		{
			name: "safe direction: prior document on the left",
			sql: `SET result = (
				COALESCE(unit.result::jsonb, '{}'::jsonb) ||
				jsonb_build_object('error_category', 'provider_unit_retryable')
			)`,
			want: false,
		},
		{
			name: "safe direction survives nesting on the right operand",
			sql: `SET result = (
				COALESCE(unit.result::jsonb, '{}'::jsonb) ||
				jsonb_build_object(
					'error_category', 'worker_lost',
					'next_retry_at', to_jsonb($4::timestamptz)
				)
			)`,
			want: false,
		},
		{
			name: "wholesale replacement touches no prior document",
			sql:  `SET result = $5::jsonb, error = NULL`,
			want: false,
		},
		{
			name: "a build_object with no merge at all",
			sql: `SET result = jsonb_build_object(
				'error_category', 'worker_lost',
				'retry_count', unit.expired_lease_retry_count + 1,
				'next_retry_at', to_jsonb($4::timestamptz)
			)`,
			want: false,
		},
		{
			name: "a KEY named result on the right is not a column read",
			sql:  `SET result = COALESCE(unit.result::jsonb, '{}'::jsonb) || jsonb_build_object('result', 'ok', 'result_count', 1)`,
			want: false,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := PreservesPriorResultCategory(test.sql); got != test.want {
				t.Fatalf("PreservesPriorResultCategory = %v, want %v for:\n%s",
					got, test.want, test.sql)
			}
		})
	}
}

// TestConcatOperandsKeepsNestedGroupsWhole pins the property the whole
// detector rests on, separately from the verdict: an operand must span a
// parenthesised group entirely rather than stopping at its first inner `)`.
func TestConcatOperandsKeepsNestedGroupsWhole(t *testing.T) {
	concats := ConcatOperands(
		`result = (jsonb_build_object('a', to_jsonb($1::text), 'b', 2) || COALESCE(unit.result, '{}'))`,
	)
	if len(concats) != 1 {
		t.Fatalf("found %d concatenations, want 1: %#v", len(concats), concats)
	}
	if want := `jsonb_build_object('a', to_jsonb($1::text), 'b', 2) `; concats[0].Left != want {
		t.Errorf("left operand = %q, want %q", concats[0].Left, want)
	}
	if want := ` COALESCE(unit.result, '{}')`; concats[0].Right != want {
		t.Errorf("right operand = %q, want %q", concats[0].Right, want)
	}
}

// TestConcatOperandsSplitsOnSiblingAssignments keeps an operand from running
// across a `,` or `=` at its own depth and picking up a `result` reference
// that belongs to a different SET clause entirely.
func TestConcatOperandsSplitsOnSiblingAssignments(t *testing.T) {
	concats := ConcatOperands(
		`SET note = 'a' || 'b', result = COALESCE(unit.result, '{}')`,
	)
	if len(concats) != 1 {
		t.Fatalf("found %d concatenations, want 1: %#v", len(concats), concats)
	}
	if want := ` 'b'`; concats[0].Right != want {
		t.Errorf("right operand = %q, want %q", concats[0].Right, want)
	}
}
