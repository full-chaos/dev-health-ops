package secrets

import (
	"bytes"
	"flag"
	"strings"
	"testing"
)

// marker is a recognisable, obviously-fake credential-shaped value:
// present only if a redaction guard fails to hold.
const marker = "MARKER-PW-7f3a9c"

func TestBindFlag_UsageNeverPrintsTheEnvValue(t *testing.T) {
	t.Setenv("TEST_DSN", "postgres://user:"+marker+"@host/db")

	set := flag.NewFlagSet("test", flag.ContinueOnError)
	var out bytes.Buffer
	set.SetOutput(&out)
	var dsn string
	BindFlag(set, &dsn, "dsn", "TEST_DSN", "a DSN")

	set.Usage()

	if strings.Contains(out.String(), marker) {
		t.Fatalf("usage output leaked the env value: %s", out.String())
	}
	if !strings.Contains(out.String(), "TEST_DSN") {
		t.Fatalf("usage output should name TEST_DSN: %s", out.String())
	}
}

func TestBindFlag_ParseErrorNeverPrintsTheEnvValue(t *testing.T) {
	t.Setenv("TEST_DSN", "postgres://user:"+marker+"@host/db")

	set := flag.NewFlagSet("test", flag.ContinueOnError)
	var out bytes.Buffer
	set.SetOutput(&out)
	var dsn string
	BindFlag(set, &dsn, "dsn", "TEST_DSN", "a DSN")

	err := set.Parse([]string{"-not-a-real-flag"})
	if err == nil {
		t.Fatal("want a parse error for an undefined flag")
	}
	if strings.Contains(out.String(), marker) {
		t.Fatalf("parse-error output leaked the env value: %s", out.String())
	}
	if !strings.Contains(out.String(), "TEST_DSN") {
		t.Fatalf("parse-error output should name TEST_DSN: %s", out.String())
	}
}

func TestResolveFlag_FallsBackToTheEnvValueWhenTheFlagIsUnset(t *testing.T) {
	t.Setenv("TEST_DSN", "postgres://user:"+marker+"@host/db")

	set := flag.NewFlagSet("test", flag.ContinueOnError)
	var dsn string
	BindFlag(set, &dsn, "dsn", "TEST_DSN", "a DSN")
	if err := set.Parse(nil); err != nil {
		t.Fatalf("Parse: %v", err)
	}

	ResolveFlag(set, &dsn, "dsn", "TEST_DSN")

	if dsn != "postgres://user:"+marker+"@host/db" {
		t.Fatalf("dsn = %q, want the env value", dsn)
	}
}

func TestResolveFlag_ExplicitFlagWinsOverTheEnvValue(t *testing.T) {
	t.Setenv("TEST_DSN", "postgres://user:"+marker+"@host/db")

	set := flag.NewFlagSet("test", flag.ContinueOnError)
	var dsn string
	BindFlag(set, &dsn, "dsn", "TEST_DSN", "a DSN")
	if err := set.Parse([]string{"-dsn", "postgres://explicit/db"}); err != nil {
		t.Fatalf("Parse: %v", err)
	}

	ResolveFlag(set, &dsn, "dsn", "TEST_DSN")

	if dsn != "postgres://explicit/db" {
		t.Fatalf("dsn = %q, want the explicit flag value", dsn)
	}
}

// TestResolveFlag_ExplicitEmptyFlagWinsOverTheEnvValue: -dsn= (explicitly
// empty) must stay empty, never silently pick up TEST_DSN, the same as it
// would with no BindFlag/ResolveFlag involved at all (an explicitly-set
// flag always wins over its own default in the flag package).
func TestResolveFlag_ExplicitEmptyFlagWinsOverTheEnvValue(t *testing.T) {
	t.Setenv("TEST_DSN", "postgres://user:"+marker+"@host/db")

	set := flag.NewFlagSet("test", flag.ContinueOnError)
	var dsn string
	BindFlag(set, &dsn, "dsn", "TEST_DSN", "a DSN")
	if err := set.Parse([]string{"-dsn="}); err != nil {
		t.Fatalf("Parse: %v", err)
	}

	ResolveFlag(set, &dsn, "dsn", "TEST_DSN")

	if dsn != "" {
		t.Fatalf("dsn = %q, want empty (an explicit -dsn= must not fall back to TEST_DSN)", dsn)
	}
}

func TestRedactedConnectError_NamesTheOperationFlagAndEnvVarNeverAValue(t *testing.T) {
	err := RedactedConnectError("connect to postgres", "postgres-uri", "POSTGRES_URI")
	if err == nil {
		t.Fatal("want a non-nil error")
	}
	got := err.Error()
	for _, want := range []string{"connect to postgres", "postgres-uri", "POSTGRES_URI"} {
		if !strings.Contains(got, want) {
			t.Fatalf("error %q should contain %q", got, want)
		}
	}
	if strings.Contains(got, marker) {
		t.Fatalf("error %q should never contain a credential-shaped value", got)
	}
}

// TestResolveFlag_EnvValueIsNeverTransformed: the environment fallback is
// used byte-for-byte, the same as the original os.Getenv-default code it
// replaces -- no trimming, no normalisation.
func TestResolveFlag_EnvValueIsNeverTransformed(t *testing.T) {
	const withPadding = "  postgres://user:" + marker + "@host/db  "
	t.Setenv("TEST_DSN", withPadding)

	set := flag.NewFlagSet("test", flag.ContinueOnError)
	var dsn string
	BindFlag(set, &dsn, "dsn", "TEST_DSN", "a DSN")
	if err := set.Parse(nil); err != nil {
		t.Fatalf("Parse: %v", err)
	}

	ResolveFlag(set, &dsn, "dsn", "TEST_DSN")

	if dsn != withPadding {
		t.Fatalf("dsn = %q, want the env value unchanged: %q", dsn, withPadding)
	}
}
