package config

import (
	"strings"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
)

// The ClickHouse Cloud login of the migrate Job (CHAOS-11995): the component
// form with DEV_HEALTH_CH_SECURE=true carries any password to the driver
// unchanged, over TLS.
func cloudComponents(password string) map[string]string {
	return map[string]string{
		"DEV_HEALTH_CH_HOST":     "abc123.eu-west-1.aws.clickhouse.cloud",
		"DEV_HEALTH_CH_PORT":     "9440",
		"DEV_HEALTH_CH_USER":     "default",
		"DEV_HEALTH_CH_PASSWORD": password,
		"DEV_HEALTH_CH_DB":       "default",
		"DEV_HEALTH_CH_SECURE":   "true",
	}
}

func TestClickHouseCloudComponentsCarryEveryReservedPasswordCharacterOverTLS(t *testing.T) {
	t.Parallel()
	for _, password := range []string{
		"pw#frag", "pw@at", "pw/slash", "pw?query", "pw:colon", "pw%25pct", "pw%zz", "pw&amp", "pw+plus",
		"pw space", "pw'quote", `pw"dq`, "pw=eq", "pw;semi", "pw[br]", "#@/?:%&+= all",
	} {
		t.Run(password, func(t *testing.T) {
			t.Parallel()
			dsn, configured, err := ResolveDSN(lookup(cloudComponents(password)), "CLICKHOUSE_URI", ClickHouseSpec)
			if err != nil || !configured {
				t.Fatalf("component form refused a valid Cloud login: configured=%v err=%v", configured, err)
			}
			options, err := clickhouse.ParseDSN(dsn.Reveal())
			if err != nil {
				t.Fatalf("the driver refused the assembled DSN: %v", err)
			}
			if options.Auth.Password != password {
				t.Fatalf("the driver reads a different password back (len %d, want len %d)", len(options.Auth.Password), len(password))
			}
			if options.Auth.Username != "default" || options.Auth.Database != "default" {
				t.Fatalf("user/db = %q/%q, want default/default", options.Auth.Username, options.Auth.Database)
			}
			if len(options.Addr) != 1 || options.Addr[0] != "abc123.eu-west-1.aws.clickhouse.cloud:9440" {
				t.Fatalf("addr = %v, want the one Cloud endpoint on 9440", options.Addr)
			}
			if options.TLS == nil {
				t.Fatal("DEV_HEALTH_CH_SECURE=true must reach the driver as TLS on")
			}
		})
	}
}

func TestClickHouseSecureUnsetOrFalseIsTheDSNWithoutTheKey(t *testing.T) {
	t.Parallel()
	noKey := ClickHouseSpec
	noKey.SecureKey = ""
	base := cloudComponents("pw#frag")
	delete(base, "DEV_HEALTH_CH_SECURE")
	want, _, err := ResolveDSNFromComponents(lookup(base), noKey)
	if err != nil {
		t.Fatalf("spec without SecureKey: %v", err)
	}
	if strings.Contains(want.Reveal(), "?") {
		t.Fatal("the DSN without a secure switch carries a query")
	}
	for name, value := range map[string]*string{"unset": nil, "empty": ptr(""), "false": ptr("false")} {
		env := cloudComponents("pw#frag")
		delete(env, "DEV_HEALTH_CH_SECURE")
		if value != nil {
			env["DEV_HEALTH_CH_SECURE"] = *value
		}
		got, _, err := ResolveDSN(lookup(env), "CLICKHOUSE_URI", ClickHouseSpec)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got.Reveal() != want.Reveal() {
			t.Fatalf("%s: DEV_HEALTH_CH_SECURE changed the DSN that a spec without the key builds", name)
		}
		options, err := clickhouse.ParseDSN(got.Reveal())
		if err != nil || options.TLS != nil {
			t.Fatalf("%s: TLS must stay off (err=%v)", name, err)
		}
	}
}

func ptr(s string) *string { return &s }

func TestClickHouseSecureTakesOnlyTrueOrFalse(t *testing.T) {
	t.Parallel()
	const canary = "v3rCanary#Pw-11995"
	for _, value := range []string{"1", "TRUE", "True", "yes", "on", " true", "true ", "secure", canary, "default", "pw#frag", `#@/?:%&+= all`} {
		env := cloudComponents(canary)
		env["DEV_HEALTH_CH_SECURE"] = value
		_, _, err := ResolveDSN(lookup(env), "CLICKHOUSE_URI", ClickHouseSpec)
		if err == nil {
			t.Fatalf("DEV_HEALTH_CH_SECURE=%q was taken", value)
		}
		if !strings.Contains(err.Error(), "DEV_HEALTH_CH_SECURE") {
			t.Fatalf("DEV_HEALTH_CH_SECURE=%q: the refusal does not name the key: %v", value, err)
		}
		if strings.Contains(err.Error(), canary) {
			t.Fatalf("DEV_HEALTH_CH_SECURE=%q: the refusal holds the password", value)
		}
		// The refusal is one fixed text, so no refused value (a misplaced credential) can reach it.
		if want := "DEV_HEALTH_CH_SECURE must be true or false; the value set is neither (not printed)"; err.Error() != want {
			t.Fatalf("DEV_HEALTH_CH_SECURE=%q: refusal %q, want the fixed text %q", value, err.Error(), want)
		}
	}
}

func TestClickHouseSecureIsAComponentKey(t *testing.T) {
	t.Parallel()
	// Alone, it is a component set with no host: refused, not ignored.
	if _, _, err := ResolveDSN(lookup(map[string]string{"DEV_HEALTH_CH_SECURE": "true"}), "CLICKHOUSE_URI", ClickHouseSpec); err == nil {
		t.Fatal("DEV_HEALTH_CH_SECURE without DEV_HEALTH_CH_HOST was not refused")
	}
	// With a non-empty pre-built CLICKHOUSE_URI: the two forms stay exclusive.
	env := cloudComponents("pw")
	env["CLICKHOUSE_URI"] = "clickhouse://dho_app:x@ch.internal:9000/default"
	if _, _, err := ResolveDSN(lookup(env), "CLICKHOUSE_URI", ClickHouseSpec); err == nil {
		t.Fatal("components + a non-empty CLICKHOUSE_URI were not refused")
	}
	// A blank CLICKHOUSE_URI (the chart's env entry over the envFrom key) counts as unset.
	env["CLICKHOUSE_URI"] = ""
	if _, configured, err := ResolveDSN(lookup(env), "CLICKHOUSE_URI", ClickHouseSpec); err != nil || !configured {
		t.Fatalf("components + a blank CLICKHOUSE_URI: configured=%v err=%v", configured, err)
	}
}
