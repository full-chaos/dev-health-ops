package engineclause

import (
	"errors"
	"reflect"
	"testing"
)

func TestParse(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		in     string
		engine string
		family string
		prefix string
		args   []string
	}{
		{"self-hosted with version", "ReplacingMergeTree(computed_at) ORDER BY (org_id, day)", "ReplacingMergeTree", "ReplacingMergeTree", "", []string{"computed_at"}},
		{"self-hosted no args", "ReplacingMergeTree ORDER BY org_id", "ReplacingMergeTree", "ReplacingMergeTree", "", nil},
		{"self-hosted empty parens", "ReplacingMergeTree() ORDER BY org_id", "ReplacingMergeTree", "ReplacingMergeTree", "", nil},
		{"bare engine column", "SharedReplacingMergeTree", "SharedReplacingMergeTree", "ReplacingMergeTree", "Shared", nil},
		{"plain MergeTree", "MergeTree ORDER BY x", "MergeTree", "MergeTree", "", nil},
		{"cloud shared with version", "SharedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', computed_at) ORDER BY (org_id, day) SETTINGS index_granularity = 8192", "SharedReplacingMergeTree", "ReplacingMergeTree", "Shared", []string{"computed_at"}},
		{"cloud shared no version", "SharedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}') ORDER BY x", "SharedReplacingMergeTree", "ReplacingMergeTree", "Shared", nil},
		{"cloud shared MergeTree", "SharedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}') ORDER BY x", "SharedMergeTree", "MergeTree", "Shared", nil},
		{"cloud aggregating", "SharedAggregatingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}') ORDER BY x", "SharedAggregatingMergeTree", "AggregatingMergeTree", "Shared", nil},
		{"cloud summing with columns", "SharedSummingMergeTree('/p', '{replica}', (a, b)) ORDER BY x", "SharedSummingMergeTree", "SummingMergeTree", "Shared", []string{"(a, b)"}},
		{"replicated with version", "ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/t', '{replica}', ver) ORDER BY x", "ReplicatedReplacingMergeTree", "ReplacingMergeTree", "Replicated", []string{"ver"}},
		{"replicated version and is_deleted", "ReplicatedReplacingMergeTree('/p', '{replica}', ver, is_deleted) ORDER BY x", "ReplicatedReplacingMergeTree", "ReplacingMergeTree", "Replicated", []string{"ver", "is_deleted"}},
		{"replicated default path no args", "ReplicatedMergeTree ORDER BY x", "ReplicatedMergeTree", "MergeTree", "Replicated", nil},
		{"replicated aggregating", "ReplicatedAggregatingMergeTree('/p', '{replica}') ORDER BY x", "ReplicatedAggregatingMergeTree", "AggregatingMergeTree", "Replicated", nil},
		{"backticked version", "ReplacingMergeTree(`computed_at`) ORDER BY x", "ReplacingMergeTree", "ReplacingMergeTree", "", []string{"`computed_at`"}},
		{"cloud backticked version", "SharedReplacingMergeTree('/p', '{replica}', `computed_at`) ORDER BY x", "SharedReplacingMergeTree", "ReplacingMergeTree", "Shared", []string{"`computed_at`"}},
		{"whitespace everywhere", "  SharedReplacingMergeTree (\n  '/p' ,\n '{replica}' ,\n computed_at\n ) ORDER BY x", "SharedReplacingMergeTree", "ReplacingMergeTree", "Shared", []string{"computed_at"}},
		{"comma and paren inside quoted path", "SharedReplacingMergeTree('/a,b)c', '{r}', v) ORDER BY x", "SharedReplacingMergeTree", "ReplacingMergeTree", "Shared", []string{"v"}},
		{"doubled and escaped quotes in path", "ReplicatedReplacingMergeTree('/a''b\\'c', '{r}', v)", "ReplicatedReplacingMergeTree", "ReplacingMergeTree", "Replicated", []string{"v"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := Parse(tc.in)
			if err != nil {
				t.Fatalf("Parse(%q): %v", tc.in, err)
			}
			if got.Name != tc.engine || got.Family != tc.family || got.Prefix != tc.prefix {
				t.Fatalf("Parse(%q) = %+v, want name %q family %q prefix %q", tc.in, got, tc.engine, tc.family, tc.prefix)
			}
			if !reflect.DeepEqual(got.Args, tc.args) {
				t.Fatalf("Parse(%q) args = %#v, want %#v", tc.in, got.Args, tc.args)
			}
		})
	}
}

func TestParseRefusesUnknownShapes(t *testing.T) {
	t.Parallel()
	for _, in := range []string{
		"",
		"   ",
		"View",
		"MaterializedView",
		"Distributed('c', 'db', 't')",
		"Memory",
		"SharedView",
		"ReplacingMergeTree(computed_at",
		"ReplacingMergeTree('unterminated)",
		"ReplacingMergeTree(computed_at,)",
		"ReplacingMergeTree(, computed_at)",
		"ReplacingMergeTree(a,,b)",
		"ReplacingMergeTree,x",
		"SharedReplacingMergeTree(computed_at)",
		"SharedReplacingMergeTree('/p', computed_at)",
		"SharedReplacingMergeTree('/p')",
		"ReplicatedReplacingMergeTree(a, b, c)",
		"ReplacingMergeTree(f(a, b)",
		"ReplacingMergeTree(a]",
	} {
		if _, err := Parse(in); !errors.Is(err, ErrShape) {
			t.Errorf("Parse(%q) error = %v, want ErrShape", in, err)
		}
	}
}

func TestVersionColumn(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in      string
		want    string
		wantErr bool
	}{
		{"ReplacingMergeTree(computed_at) ORDER BY x", "computed_at", false},
		{"ReplacingMergeTree(`computed_at`) ORDER BY x", "computed_at", false},
		{"ReplacingMergeTree(\"computed_at\")", "computed_at", false},
		{"ReplacingMergeTree ORDER BY x", "", false},
		{"ReplacingMergeTree() ORDER BY x", "", false},
		{"SharedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', computed_at) ORDER BY x", "computed_at", false},
		{"SharedReplacingMergeTree('/p', '{replica}', `computed_at`)", "computed_at", false},
		{"SharedReplacingMergeTree('/p', '{replica}') ORDER BY x", "", false},
		{"ReplicatedReplacingMergeTree('/p', '{replica}', ver, is_deleted)", "", true},
		{"ReplacingMergeTree(ver, is_deleted)", "", true},
		{"SharedMergeTree('/p', '{replica}') ORDER BY x", "", true},
		{"SharedAggregatingMergeTree('/p', '{replica}') ORDER BY x", "", true},
		{"ReplacingMergeTree(toDate(computed_at))", "", true},
		{"ReplacingMergeTree(a b)", "", true},
	}
	for _, tc := range cases {
		clause, err := Parse(tc.in)
		if err != nil {
			t.Fatalf("Parse(%q): %v", tc.in, err)
		}
		got, err := clause.VersionColumn()
		if (err != nil) != tc.wantErr || got != tc.want {
			t.Errorf("VersionColumn(%q) = %q, %v; want %q, error %v", tc.in, got, err, tc.want, tc.wantErr)
		}
	}
}

func TestIsReplacingOn(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in     string
		column string
		want   bool
	}{
		{"ReplacingMergeTree(updated_at) ORDER BY x", "updated_at", true},
		{"ReplacingMergeTree(`updated_at`) ORDER BY x", "updated_at", true},
		{"SharedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', updated_at) ORDER BY x", "updated_at", true},
		{"ReplicatedReplacingMergeTree('/p', '{replica}', updated_at) ORDER BY x", "updated_at", true},
		{"ReplacingMergeTree(updated_at) ORDER BY x", "last_synced", false},
		{"SharedReplacingMergeTree('/p', '{replica}', other) ORDER BY x", "updated_at", false},
		{"SharedReplacingMergeTree('/p', '{replica}') ORDER BY x", "updated_at", false},
		{"SharedReplacingMergeTree('/p', '{replica}', updated_at, is_deleted)", "updated_at", false},
		{"SharedMergeTree('/p', '{replica}') ORDER BY x", "updated_at", false},
		{"SharedAggregatingMergeTree('/p', '{replica}', updated_at)", "updated_at", false},
		{"ReplacingMergeTree(toDate(updated_at))", "updated_at", false},
	}
	for _, tc := range cases {
		clause, err := Parse(tc.in)
		if err != nil {
			t.Fatalf("Parse(%q): %v", tc.in, err)
		}
		if got := clause.IsReplacingOn(tc.column); got != tc.want {
			t.Errorf("IsReplacingOn(%q, %q) = %v, want %v", tc.in, tc.column, got, tc.want)
		}
	}
}

func TestCanonicalizeDDL(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		in   string
		want string
	}{
		{
			"self-hosted unchanged",
			"CREATE TABLE t (a UInt8) ENGINE = ReplacingMergeTree(ingest_revision) PRIMARY KEY (org_id, id) ORDER BY (org_id, id)",
			"CREATE TABLE t (a UInt8) ENGINE = ReplacingMergeTree(ingest_revision) PRIMARY KEY (org_id, id) ORDER BY (org_id, id)",
		},
		{
			"cloud shared",
			"CREATE TABLE t (a UInt8) ENGINE = SharedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', ingest_revision) PRIMARY KEY (org_id, id) ORDER BY (org_id, id) SETTINGS index_granularity = 8192",
			"CREATE TABLE t (a UInt8) ENGINE = ReplacingMergeTree(ingest_revision) PRIMARY KEY (org_id, id) ORDER BY (org_id, id) SETTINGS index_granularity = 8192",
		},
		{
			"replicated",
			"CREATE TABLE t (a UInt8)\nENGINE = ReplicatedReplacingMergeTree('/p/{shard}', '{replica}', source_version_at)\nORDER BY (org_id, id)",
			"CREATE TABLE t (a UInt8)\nENGINE = ReplacingMergeTree(source_version_at)\nORDER BY (org_id, id)",
		},
		{
			"shared without version",
			"CREATE TABLE t (a UInt8) ENGINE = SharedReplacingMergeTree('/p', '{replica}') ORDER BY x",
			"CREATE TABLE t (a UInt8) ENGINE = ReplacingMergeTree ORDER BY x",
		},
		{"no engine clause", "CREATE VIEW v AS SELECT 1", "CREATE VIEW v AS SELECT 1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := CanonicalizeDDL(tc.in)
			if err != nil {
				t.Fatalf("CanonicalizeDDL: %v", err)
			}
			if got != tc.want {
				t.Fatalf("CanonicalizeDDL =\n%q\nwant\n%q", got, tc.want)
			}
		})
	}
}

func TestCanonicalizeDDLRefusesUnknownShared(t *testing.T) {
	t.Parallel()
	for _, in := range []string{
		"CREATE TABLE t (a UInt8) ENGINE = SharedReplacingMergeTree(ingest_revision) ORDER BY x",
		"CREATE TABLE t (a UInt8) ENGINE = SharedReplacingMergeTree('/p', '{replica}', ingest_revision ORDER BY x",
		"CREATE TABLE t (a UInt8) ENGINE = Distributed('c', 'db', 't')",
	} {
		if _, err := CanonicalizeDDL(in); !errors.Is(err, ErrShape) {
			t.Errorf("CanonicalizeDDL(%q) error = %v, want ErrShape", in, err)
		}
	}
}
