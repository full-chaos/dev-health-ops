package gqlgenguard

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestGenerateRefusesToRevertAStaleRecordsHandEdits: the record says a file
// carries a deliberate hand-edit. Whether the file still holds exactly the
// bytes the record was written from decides nothing -- regenerating reverts
// whatever is there -- so a record that has fallen behind must make the write
// verb MORE careful, not less.
func TestGenerateRefusesToRevertAStaleRecordsHandEdits(t *testing.T) {
	f := guardFixture(t).withModuleFiles()
	opts := guardOptions(f, &fakeGenerator{fn: rewriteOutputs("generated")})
	opts.Report = new(strings.Builder)
	if _, err := UpdateDriftRecord(context.Background(), opts); err != nil {
		t.Fatalf("write the record: %v", err)
	}
	recorded := f.read("contracts/expected-drift.record")
	if !strings.Contains(recorded, "digest drift gen/generated.go") {
		t.Fatalf("the fixture's record carries no drift to revert:\n%s", recorded)
	}

	// The hand-edit moves on: the record now describes bytes the tree no
	// longer has.
	f.write("gen/generated.go", f.read("gen/generated.go")+"\n// a later hand-edit\n")

	var report strings.Builder
	opts.Report = &report
	before := f.digests()
	_, err := Generate(context.Background(), opts)
	logCell(t, err, "accepted")
	if err == nil || !strings.Contains(err.Error(), "records as deliberate") {
		t.Fatalf("want the recorded-hand-edit refusal, got %v", err)
	}
	if !strings.Contains(err.Error(), "gen/generated.go") {
		t.Fatalf("the refusal does not name the file: %v", err)
	}
	// Both digests, so a stale record is visible rather than merely implied.
	if !strings.Contains(err.Error(), "recorded  ") || !strings.Contains(err.Error(), "in the tree ") {
		t.Fatalf("the refusal does not show the recorded and the current bytes: %v", err)
	}
	assertUnchanged(t, before, f.digests(), "generate over a stale record")

	opts.RevertRecorded = true
	var applied strings.Builder
	opts.Report = &applied
	if _, err := Generate(context.Background(), opts); err != nil {
		t.Fatalf("generate under -revert-recorded: %v\n%s", err, applied.String())
	}
	if !strings.Contains(applied.String(), "reverting ") {
		t.Fatalf("the applied run did not name what it reverted:\n%s", applied.String())
	}
}

// TestTheRecordCannotBeAPathTheConfigurationNames is the pairwise cell for the
// two operator inputs that name paths in the same tree: -record and the gqlgen
// config. Where they name the same path, the record is read from -- and with
// -update written over -- a file the generator owns.
func TestTheRecordCannotBeAPathTheConfigurationNames(t *testing.T) {
	cells := []struct {
		shape   string
		record  string
		wantErr string
	}{
		{"G25-01 the record is a declared output", "gen/generated.go", "the declared output of"},
		{"G25-02 the record is the schema", "schema.graphql", "a schema"},
		{"G25-03 the record is the gqlgen config", "gqlgen.yml", "the gqlgen config"},
		{"G25-04 the record is a declared output reached with a redundant ./", "./gen/model/models_gen.go", "the declared output of"},
		{"G25-05 the record reaches a declared output through an in-module link", "genlink/generated.go", "the declared output of"},
		{"G25-06 the record carries a `..` after a directory name", "gen/../contracts/expected-drift.record", "has a `..` after a directory name"},
		{"G25-07 the record has a LEADING ../ only, which climbs from the module root", "../outside.record", "path escapes from parent"},
		{"G25-08 the canonical record, named by nothing in the config", "contracts/expected-drift.record", ""},
	}
	for _, c := range cells {
		t.Run(c.shape, func(t *testing.T) {
			f := guardFixture(t).withModuleFiles()
			if strings.HasPrefix(c.record, "genlink/") {
				if err := os.Symlink("gen", filepath.Join(f.dir, "genlink")); err != nil {
					t.Fatal(err)
				}
			}
			opts := guardOptions(f, &fakeGenerator{fn: rewriteOutputs("generated")})
			opts.DriftPath = c.record
			var report strings.Builder
			opts.Report = &report
			before := f.digests()

			_, err := UpdateDriftRecord(context.Background(), opts)
			logCell(t, err, "accepted")
			if c.wantErr == "" {
				if err != nil {
					t.Fatalf("the canonical record was refused: %v\n%s", err, report.String())
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Fatalf("want a refusal naming %q, got %v", c.wantErr, err)
			}
			assertUnchanged(t, before, f.digests(), c.shape+" under -update")

			// check-drift reads the same file, and refuses for the same reason.
			_, err = CheckDrift(context.Background(), opts)
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Fatalf("check-drift accepted the same record path: %v", err)
			}
			assertUnchanged(t, before, f.digests(), c.shape+" under check-drift")
		})
	}
}

// TestTheWritingVerbFailsClosedOnTheRecordState: the protection this tool
// exists to give belongs to the record STATE, not to the verb. Being unable to
// READ the record is the state in which the guard knows least about what it is
// about to destroy, so it is a refusal — not permission. `check-drift` refused
// these states from the start; `generate` treating them as "nothing is
// recorded, nothing can be destroyed" is how a mistyped flag reverted 152
// recorded lines and exited 0.
func TestTheWritingVerbFailsClosedOnTheRecordState(t *testing.T) {
	const header = "# gqlgen expected-drift record. GENERATED -- rewrite with:\n#     go run ./cmd/gqlgen-guard check-drift -update\n"
	cells := []struct {
		shape   string
		record  string // "" leaves the recorded state alone
		prepare func(t *testing.T, f *fixture, opts *Options)
		wantErr string
	}{
		{shape: "G26-01 the record is absent", wantErr: "no expected-drift record at"},
		{shape: "G26-02 the record path is mistyped", prepare: func(t *testing.T, f *fixture, o *Options) {
			writeRecord(t, f, o)
			o.DriftPath = "contracts/expected-drift.recrd"
		}, wantErr: "no expected-drift record at"},
		{shape: "G26-03 the record is empty", prepare: func(t *testing.T, f *fixture, o *Options) {
			writeRecord(t, f, o)
			f.write(o.driftPath(), "")
		}, wantErr: "carries no digest line"},
		{shape: "G26-04 the record kept its header and lost its digest table", prepare: func(t *testing.T, f *fixture, o *Options) {
			writeRecord(t, f, o)
			f.write(o.driftPath(), header)
		}, wantErr: "carries no digest line"},
		{shape: "G26-05 the record covers one of the two files about to be overwritten", prepare: func(t *testing.T, f *fixture, o *Options) {
			writeRecord(t, f, o)
			// Drop one file's digest line, and make the other's status
			// something other than `drift`, so the recorded-hand-edit refusal
			// has nothing to say and only the coverage check can speak.
			var kept []string
			for _, l := range strings.Split(f.read(o.driftPath()), "\n") {
				if strings.HasPrefix(l, "digest ") && strings.Contains(l, "models_gen.go") {
					continue
				}
				kept = append(kept, strings.Replace(l, "digest drift gen/generated.go", "digest same gen/generated.go", 1))
			}
			f.write(o.driftPath(), strings.Join(kept, "\n"))
		}, wantErr: "has no digest line for 1 file(s) this run would overwrite"},
		{shape: "G26-06 the record describes other bytes than a file now holds (an edit made since the last -update)", prepare: func(t *testing.T, f *fixture, o *Options) {
			writeRecord(t, f, o)
			var kept []string
			for _, l := range strings.Split(f.read(o.driftPath()), "\n") {
				kept = append(kept, strings.Replace(strings.Replace(l,
					"digest drift gen/generated.go", "digest same gen/generated.go", 1),
					"digest drift gen/model/models_gen.go", "digest same gen/model/models_gen.go", 1))
			}
			f.write(o.driftPath(), strings.Join(kept, "\n"))
			// The hand-edit the record cannot see, made after it was written.
			f.write("gen/generated.go", f.read("gen/generated.go")+"\n// an edit made since the record\n")
		}, wantErr: "describes other bytes than"},
		{shape: "G26-07 the record is present and covers every file (the ordinary case)", prepare: func(t *testing.T, f *fixture, o *Options) {
			writeRecord(t, f, o)
		}, wantErr: ""},
	}
	for _, c := range cells {
		t.Run(c.shape, func(t *testing.T) {
			f := guardFixture(t).withModuleFiles()
			opts := guardOptions(f, &fakeGenerator{fn: rewriteOutputs("generated")})
			var report strings.Builder
			opts.Report = &report
			if c.prepare != nil {
				c.prepare(t, f, &opts)
			}
			before := f.digests()

			_, err := Generate(context.Background(), opts)
			logCell(t, err, "accepted")
			if c.wantErr == "" {
				// The ordinary case still refuses, but for the RECORDED
				// hand-edits, which is the refusal this cell is not about.
				if err != nil && !strings.Contains(err.Error(), "records as deliberate") {
					t.Fatalf("a complete record produced the wrong refusal: %v", err)
				}
			} else {
				if err == nil || !strings.Contains(err.Error(), c.wantErr) {
					t.Fatalf("want a refusal naming %q, got %v", c.wantErr, err)
				}
				assertUnchanged(t, before, f.digests(), c.shape)
			}

			// -revert-recorded is the operator's override, and the ONLY one:
			// the same state writes when they say so.
			opts.RevertRecorded = true
			opts.Report = new(strings.Builder)
			res, rerr := Generate(context.Background(), opts)
			if rerr != nil {
				t.Fatalf("-revert-recorded did not override %s: %v", c.shape, rerr)
			}
			if len(res.Applied) == 0 {
				t.Fatalf("-revert-recorded applied nothing, so the override proves nothing")
			}
		})
	}
}

// writeRecord puts a complete, current expected-drift record in the fixture.
func writeRecord(t *testing.T, f *fixture, o *Options) {
	t.Helper()
	rec := *o
	rec.Report = io.Discard
	rec.RevertRecorded = false
	if _, err := UpdateDriftRecord(context.Background(), rec); err != nil {
		t.Fatalf("write the fixture's record: %v", err)
	}
}

// TestUpdateNeverReplacesAFileThatIsNotARecord: -update is the one path in this
// package that writes to the tree outside `generate`, and its destination comes
// from a flag. Pointed at any tracked file it used to overwrite it with the
// record and report `wrote <path>`.
func TestUpdateNeverReplacesAFileThatIsNotARecord(t *testing.T) {
	cells := []struct {
		shape   string
		record  string
		body    string
		wantErr string
	}{
		{"G27-01 the destination is a go.mod", "go.mod", "module example.com/fixture\n\ngo 1.25\n", "is not an expected-drift record"},
		{"G27-02 the destination is a README", "docs/README.md", "# notes\n\nhand-written.\n", "is not an expected-drift record"},
		{"G27-03 the destination is a file whose first line only looks close", "contracts/near.record", "# gqlgen expected-drift record, sort of\n", "is not an expected-drift record"},
		{"G27-04 the destination does not exist yet", "contracts/fresh.record", "", ""},
	}
	for _, c := range cells {
		t.Run(c.shape, func(t *testing.T) {
			f := guardFixture(t).withModuleFiles()
			opts := guardOptions(f, &fakeGenerator{fn: rewriteOutputs("generated")})
			opts.DriftPath = c.record
			var report strings.Builder
			opts.Report = &report
			if c.body != "" {
				f.write(c.record, c.body)
			}
			before := f.digests()

			_, err := UpdateDriftRecord(context.Background(), opts)
			logCell(t, err, "accepted")
			if c.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), c.wantErr) {
					t.Fatalf("want a refusal naming %q, got %v", c.wantErr, err)
				}
				if !strings.Contains(err.Error(), c.record) {
					t.Fatalf("the refusal does not name the destination: %v", err)
				}
				assertUnchanged(t, before, f.digests(), c.shape)
				return
			}
			if err != nil {
				t.Fatalf("a fresh destination was refused: %v", err)
			}
			if !strings.HasPrefix(f.read(c.record), "# gqlgen expected-drift record.") {
				t.Fatalf("the record was not written to %s", c.record)
			}
			// And a record IS replaceable: the second -update rewrites it.
			if _, err := UpdateDriftRecord(context.Background(), opts); err != nil {
				t.Fatalf("a destination that already holds a record was refused: %v", err)
			}
			t.Logf("CELL-OUTPUT: accepted: %s written, then rewritten in place", c.record)
		})
	}
}
