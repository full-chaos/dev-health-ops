package ownermove

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestOptionsValidate(t *testing.T) {
	valid := Options{Schemas: []string{"public"}, DatabaseOwner: true, From: "devhealth", To: "devhealth_migrator", Mode: Apply}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid options refused: %v", err)
	}
	for name, mutate := range map[string]func(*Options){
		"no schema":          func(o *Options) { o.Schemas = nil },
		"a schema twice":     func(o *Options) { o.Schemas = []string{"public", "public"} },
		"a quoted schema":    func(o *Options) { o.Schemas = []string{`pub"lic`} },
		"a reserved name":    func(o *Options) { o.To = "user" },
		"no from":            func(o *Options) { o.From = "" },
		"no to":              func(o *Options) { o.To = "" },
		"from equals to":     func(o *Options) { o.To = o.From },
		"an unknown mode":    func(o *Options) { o.Mode = Mode(9) },
		"a negative timeout": func(o *Options) { o.LockTimeout = -time.Second },
		"a sub-ms timeout":   func(o *Options) { o.LockTimeout = time.Microsecond },
	} {
		options := valid
		options.Schemas = append([]string(nil), valid.Schemas...)
		mutate(&options)
		if err := options.Validate(); !errors.Is(err, ErrOwnerMove) {
			t.Errorf("%s: Validate = %v, want ErrOwnerMove", name, err)
		}
	}
}

func TestReportLine(t *testing.T) {
	report := Report{Mode: Apply, Applied: true, Schemas: []string{"public"}, DatabaseOwner: true,
		Moved:       map[string]int{"table": 125, "database": 1, "materialized view": 2},
		Followed:    map[string]int{"index": 469, "owned sequence": 3},
		GrantorRows: 1500, OldGrantorRowsLeft: 0,
		DefaultACLs: []DefaultACL{{Schema: "river", ObjectType: "r", ACL: "{devhealth_queue=arwd/devhealth}"}}}
	want := "owner-move mode=apply applied=true schemas=[public] database_owner=true moved=[database:1,materialized_view:2,table:125] " +
		"followed=[index:469,owned_sequence:3] extension_skipped=0 grantor_rows=1500 old_grantor_rows_left=0 default_acl=[river/r:{devhealth_queue=arwd/devhealth}]"
	if got := report.Line(); got != want {
		t.Fatalf("line\n got %s\nwant %s", got, want)
	}
	report.Mode, report.Applied, report.OldGrantorRowsLeft = Plan, false, -1
	if got := report.Line(); !strings.Contains(got, "mode=plan applied=false") || !strings.Contains(got, "old_grantor_rows_left=n/a") {
		t.Fatalf("plan line %s", got)
	}
}

func TestPinnedRole(t *testing.T) {
	lookup := func(values map[string]string) func(string) (string, bool) {
		return func(key string) (string, bool) { v, ok := values[key]; return v, ok }
	}
	if role, err := PinnedRole(lookup(nil)); role != "" || err != nil {
		t.Fatalf("unset: %q %v", role, err)
	}
	if role, err := PinnedRole(lookup(map[string]string{EnvOwnerRole: "  "})); role != "" || err != nil {
		t.Fatalf("blank: %q %v", role, err)
	}
	if role, err := PinnedRole(lookup(map[string]string{EnvOwnerRole: " devhealth_migrator "})); role != "devhealth_migrator" || err != nil {
		t.Fatalf("set: %q %v", role, err)
	}
	if _, err := PinnedRole(lookup(map[string]string{EnvOwnerRole: "Bad-Name"})); !errors.Is(err, ErrOwnerPin) {
		t.Fatalf("a bad name: %v", err)
	}
}

func TestAlterStatementCoversEveryMovableKind(t *testing.T) {
	for _, kind := range []string{"table", "view", "materialized view", "sequence", "foreign table", "function", "procedure",
		"aggregate", "type", "domain", "collation", "statistics", "text search dictionary", "text search configuration"} {
		if _, ok := alterStatement(kind, "s.x", "o"); !ok {
			t.Errorf("no ALTER for %s", kind)
		}
	}
	for kind := range unsupportedKinds {
		if _, ok := alterStatement(kind, "s.x", "o"); ok {
			t.Errorf("an ALTER for the unsupported kind %s", kind)
		}
	}
}
