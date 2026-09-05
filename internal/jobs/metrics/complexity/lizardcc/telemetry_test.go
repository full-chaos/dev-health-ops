package lizardcc

import "testing"

// fakeDropLogger captures every Warn call for TestLoggerFiresOnDropPaths to
// assert against -- CHAOS-5156's review checklist ("add telemetry and debug
// logs... a swallowed error is a review finding") is only actually honored
// if something PROVES the logger fires, not just that it compiles.
type fakeDropLogger struct{ calls []string }

func (f *fakeDropLogger) Warn(msg string, args ...any) { f.calls = append(f.calls, msg) }

// TestLoggerFiresOnDropPaths proves Logger.Warn is actually invoked for
// both of this package's silent-drop paths: a `#lizard forgive` comment
// (Context.EndOfFunction) and a "GENERATED CODE" marker
// (Context.HandleCommentDirectives). Restores the real Logger afterward so
// this test cannot leak state into any other test in the package.
func TestLoggerFiresOnDropPaths(t *testing.T) {
	fl := &fakeDropLogger{}
	old := Logger
	Logger = fl
	t.Cleanup(func() { Logger = old })

	if _, _, err := Analyze("x.cpp", "// #lizard forgive\nint f() { return 0; }\n"); err != nil {
		t.Fatalf("Analyze: %v", err)
	}
	if len(fl.calls) != 1 {
		t.Fatalf("forgive: got %d Warn call(s), want 1: %v", len(fl.calls), fl.calls)
	}

	fl.calls = nil
	if _, _, err := Analyze("x.cpp", "int f() { return 0; }\n/* GENERATED CODE */\nint g() { return 0; }\n"); err != nil {
		t.Fatalf("Analyze: %v", err)
	}
	if len(fl.calls) != 1 {
		t.Fatalf("generated code: got %d Warn call(s), want 1: %v", len(fl.calls), fl.calls)
	}
}

// TestLoggerNilIsNoOp proves the default (unset) Logger never panics --
// every AnalyzerFunc in this package must work with zero telemetry wiring,
// matching the codebase's other optional-logger conventions.
func TestLoggerNilIsNoOp(t *testing.T) {
	old := Logger
	Logger = nil
	t.Cleanup(func() { Logger = old })

	if _, _, err := Analyze("x.cpp", "// #lizard forgive\nint f() { return 0; }\n"); err != nil {
		t.Fatalf("Analyze: %v", err)
	}
}
