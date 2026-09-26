package synccli

import "sort"

// syncTargetCorpus is the command-line corpus the live oracle runs. It is
// generated, not hand-picked, along three axes that each found real
// divergences in earlier ports of this kind: every (target x provider) pair
// under several environments, every option spelling argparse accepts, and
// every value shape of every typed option, each in one fixed context so a
// difference is attributable to the varied element.
func syncTargetCorpus(keyFile string) []oracleCase {
	var corpus []oracleCase
	add := func(env map[string]string, target string, args ...string) {
		corpus = append(corpus, oracleCase{Args: append([]string{target}, args...), Env: env})
	}
	ch := map[string]string{"CLICKHOUSE_URI": "clickhouse://ch:ch@localhost:8123/default"}
	with := func(base map[string]string, pairs ...string) map[string]string {
		out := map[string]string{}
		for k, v := range base {
			out[k] = v
		}
		for i := 0; i+1 < len(pairs); i += 2 {
			out[pairs[i]] = pairs[i+1]
		}
		return out
	}

	// Axis 1: every target x provider under the environments that steer the
	// credential, org and sink rules.
	envs := map[string]map[string]string{
		"none":     {},
		"ch":       ch,
		"ch+org":   with(ch, "ORG_ID", "envorg"),
		"ch+empty": with(ch, "ORG_ID", ""),
		"ch+ghtok": with(ch, "GITHUB_TOKEN", "envtok", "GITHUB_URL", "https://ghe.example"),
		"ch+ghapp": with(ch, "GITHUB_APP_ID", "1", "GITHUB_APP_PRIVATE_KEY_PATH", keyFile, "GITHUB_APP_INSTALLATION_ID", "9", "GITHUB_BASE_URL", "https://ghe2.example"),
		"ch+ghbad": with(ch, "GITHUB_TOKEN", "envtok", "GITHUB_APP_ID", "1"),
		"ch+ghdb":  with(ch, "POSTGRES_URI", "postgresql://p", "ORG_ID", "envorg"),
		"ch+gldb":  with(ch, "DATABASE_URI", "postgresql://p"),
		"ch+gltok": with(ch, "GITLAB_TOKEN", "gltok", "GITLAB_URL", "https://gl.example"),
		"ch+glemp": with(ch, "GITLAB_TOKEN", "gltok", "GITLAB_URL", ""),
	}
	envNames := make([]string, 0, len(envs))
	for name := range envs {
		envNames = append(envNames, name)
	}
	sort.Strings(envNames)
	providerLines := [][]string{
		{"--provider", "local"},
		{"--provider", "local", "--repo-path", "/srv/repo", "--since", "2026-01-02"},
		{"--provider", "github", "--owner", "o", "--repo", "r", "--auth", "tok"},
		{"--provider", "github", "--owner", "o", "--repo", "r"},
		{"--provider", "github", "-s", "org/*", "--group", "g", "--max-repos", "5", "--batch-size", "3", "--max-concurrent", "2", "--rate-limit-delay", "2.5", "--use-async", "--auth", "tok"},
		{"--provider", "github", "--search", "u/*", "--owner", "someone", "--auth", "tok"},
		{"--provider", "github", "--owner", "o", "--auth", "tok"},
		{"--provider", "gitlab", "--project-id", "7", "--auth", "t"},
		{"--provider", "gitlab", "--project-id", "7"},
		{"--provider", "gitlab", "--auth", "t"},
		{"--provider", "gitlab", "-s", "grp/*", "--group", "grp", "--max-repos", "4", "--gitlab-url", "https://self.example", "--auth", "t"},
		{"--provider", "gitlab", "--project-id", "0", "--auth", "t"},
		{"--provider", "synthetic"},
		{"--provider", "synthetic", "--repo-name", "demo/x"},
	}
	for _, target := range Targets {
		for _, envName := range envNames {
			for _, line := range providerLines {
				add(envs[envName], target, line...)
			}
		}
	}

	// Axis 2: option spellings and argparse behaviours, in a github single
	// context with credentials so only the spelling varies.
	base := []string{"--provider", "github", "--owner", "o", "--repo", "r", "--auth", "tok"}
	spell := func(extra ...string) { add(ch, "git", append(append([]string{}, base...), extra...)...) }
	for _, extra := range [][]string{
		{"--since=2026-01-02"}, {"--sinc", "2026-01-02"}, {"--s", "2026-01-02"}, {"--b", "2026-01-02"}, {"--back", "3"},
		{"--backf=4"}, {"--bef", "2026-02-02"}, {"--da", "2026-01-02"}, {"--dat", "2026-01-02"}, {"--day", "2026-01-02"},
		{"--day", "2026-01-02", "--date", "2026-03-03"}, {"--date", "2026-03-03", "--before", "2026-05-05"},
		{"--day", "2026-01-02", "--backfill", "3"}, {"--date", "2026-01-02", "--since", "2026-01-01"},
		{"--s", "2026-01-02", "-h"}, {"-h", "--d", "x"}, {"--d", "x", "--help"}, {"--help", "--o"}, {"--d", "-h"}, {"-h", "--s"},
		{"--d", "x"}, {"--pro", "gitlab"}, {"--p", "gitlab"}, {"--o", "x"}, {"--org=x"}, {"--or", "x"}, {"--org", ""},
		{"--org"}, {"--auth"}, {"--auth", "--since"}, {"--auth", "-5"}, {"--owner", "-x"}, {"--owner=-x"},
		{"--max-commits-per-repo", "-5"}, {"--max-commits-per-repo=-5"}, {"--backfill", "-3"},
		{"-s"}, {"-s", "x"}, {"-sx"}, {"-s=x"}, {"-s", "org/*"}, {"-sorg/*"}, {"-s", ""},
		{"-m", "x"}, {"-mx"}, {"-l", "x"}, {"--llm-provider", "x"}, {"--model=x"}, {"--log-level", "DEBUG"},
		{"--use-async"}, {"--use-async=1"}, {"--use-async=", ""}, {"--use"}, {"--use-a", "x"},
		{"--defer-finalize"}, {"--defer"}, {"--defer-finalize=1"},
		{"-h"}, {"--help"}, {"-hx"}, {"-hs"}, {"-hs", "x"}, {"-h=x"}, {"-h-x"}, {"-hs=x"}, {"-hh"}, {"-hsx"}, {"-hm", "y"}, {"-h", "--bogus"}, {"--bogus", "-h"}, {"--help", "--since", "bad"},
		{"--since", "bad", "--help"}, {"--bogus"}, {"--bogus=1"}, {"-q"}, {"-x", "1"}, {"-1"}, {"-1.5"}, {"-.5"}, {"-1e5"}, {"-1_0"}, {"-1."}, {"-.5e-3"}, {"-\u0663"}, {"-1e"}, {"-1e+5", "x"},
		{"positional"}, {"positional", "--since", "2026-01-02"}, {"--"}, {"--", "x"}, {"--since", "--", "2026-01-02"},
		{"--", "--since", "2026-01-02"}, {"a b"}, {"--since", "2026-01-02", "a b"},
		{"--repo", "r2"}, {"--repo=r2", "--repo", "r3"}, {"--owner", ""}, {"--repo", ""},
		{"--sink", "clickhouse"}, {"--sink", "CLICKHOUSE"}, {"--sink", " auto "}, {"--sink", "mongo"}, {"--sink", "SQLite"},
		{"--sink", "postgres"}, {"--sink", "both"}, {"--sink", "elastic"}, {"--sink", ""}, {"--sink", "\u00a0auto\u00a0"}, {"--sink", "\x1fauto\x1c"},
		{"--sink"}, {"--sink=auto"}, {"--sin", "auto"},
		{"--analytics-db", "clickhouse+native://h"}, {"--analytics-db", "clickhouse+http://h"}, {"--analytics-db", "clickhouse+https://h"},
		{"--analytics-db", "CLICKHOUSE://h"}, {"--analytics-db", "postgres://h"}, {"--analytics-db", "clickhouse"},
		{"--analytics-db", "clickhouse:"}, {"--analytics-db", "://h"}, {"--analytics-db", "1ab://h"}, {"--analytics-db", " clickhouse://h"},
		{"--analytics-db", "click\nhouse://h"}, {"--analytics-db", "clickhouse\t://h"}, {"--analytics-db", "clickhouse+x://h"},
		{"--analytics-db", ""}, {"--analytics-db", "a.b-c+d://h"}, {"--analytics-db", "\x01clickhouse://h"},
		{"--analytics", "clickhouse://h"}, {"--a", "clickhouse://h"}, {"--an", "clickhouse://h"},
		{"--db", "postgresql://p"}, {"--db", ""}, {"--db=postgresql://p", "--org", "o"},
		{"--provider", "local"}, {"--provider", "GitHub"}, {"--provider=", ""}, {"--provider", "bitbucket"}, {"--provider"},
		{"--repo-path", "/x"}, {"--repo-path="}, {"--gitlab-url", "https://x"}, {"--gitlab", "https://x"},
	} {
		spell(extra...)
	}
	// No provider at all, and a target-less line.
	add(ch, "git")
	add(ch, "git", "--since", "2026-01-02")
	add(ch, "prs", "--provider")

	// Axis 3: every value shape of every typed option, one option at a time,
	// in a fixed valid context (github batch so the batch options show).
	batch := []string{"--provider", "github", "-s", "o/*", "--auth", "tok"}
	typedOptions := []string{"--batch-size", "--max-concurrent", "--max-repos", "--max-commits-per-repo", "--backfill"}
	intShapes := []string{"0", "1", "2", "3", "10", "100", "256", "257", "1000", "-1", "-5", "+3", " 4 ", "\t5\n", "1_0", "1__0", "_1", "1_", "01", "0x10", "9223372036854775808", "-9223372036854775809", "18446744073709551618", "99999999999999999999999999", "\u00a05\u00a0", "\u20035\u2003", "１２", "+ 5", "5 5", "--5", "+-5",
		"1.5", "1e3", "abc", "", " ", "--", "9223372036854775807", "-9223372036854775808", "٣"}
	for _, option := range typedOptions {
		for _, shape := range intShapes {
			for _, target := range []string{"git", "cicd"} {
				add(ch, target, append(append([]string{}, batch...), option+"="+shape)...)
				add(ch, target, append(append([]string{}, batch...), option, shape)...)
			}
		}
	}
	// max-commits interacts with the window: explicit shapes with and without one.
	for _, window := range [][]string{nil, {"--since", "2026-01-02"}, {"--backfill", "2"}, {"--backfill", "1"}, {"--before", "2026-02-02"}, {"--day", "2026-01-02"}} {
		for _, shape := range []string{"", "0", "7"} {
			line := append([]string{}, batch...)
			line = append(line, window...)
			if shape != "" {
				line = append(line, "--max-commits-per-repo", shape)
			}
			add(ch, "git", line...)
			add(ch, "git", append([]string{"--provider", "gitlab", "--project-id", "3", "--auth", "t"}, append(window, shapeArgs(shape)...)...)...)
		}
	}
	floatShapes := []string{"0", "1", "1.0", "2.5", ".5", "5.", "1e3", "1E-3", "1e400", "-1", "+2.5", "1_0.5", "1__0", "_1", "1_", "inf", "-Infinity", "NaN", "nan", "+inf", "infinit",
		"1e", "e1", "0x1p3", "++inf", "+-inf", "-+inf", "\u0130NF", "iNf", "INFINITY", "infinit", "-nan", "+NaN", "nan_", "n_an", "1_e5", "1e_5", "1e5_", "\u00a01.5\u00a0", "１.５", "1．5", "1e+", "+.5", "-.5e-3", ".e3", ".", "+", "-", "1e3.5", "0e0", "-0", "-0.0", "1e-5", "1e15", "1e16", "1e21", "123456789012345678", "abc", "", " 3 ", "1,5", "1.5.2", "٣"}
	for _, shape := range floatShapes {
		add(ch, "git", append(append([]string{}, batch...), "--rate-limit-delay="+shape)...)
		add(ch, "git", append(append([]string{}, batch...), "--rate-limit-delay", shape)...)
	}
	dateShapes := []string{"2026-01-02", "20260102", "2026-W01-1", "2026W011", "2026-W01", "2026W01", "2026-W53", "2026-W53-7", "2020-W53-7", "2025-W53", "2025-W53-1", "2025W531", "2026-W54", "2019-W52-7", "2019-W01-1", "2026-W00", "2026-W01-0", "2026-W01-8",
		"2026-13-01", "2026-02-30", "2024-02-29", "2026-1-2", "26-01-02", "0000-01-01", "0001-01-01", "9999-12-31", "2026-01-02T00:00:00", "2026-01-02 ", " 2026-01-02",
		"2026/01/02", "", "today", "2026-01", "2026", "20260230", "2026-w01-1", "２０２６-01-02"}
	for _, option := range []string{"--since", "--before", "--day", "--date"} {
		for _, shape := range dateShapes {
			add(ch, "git", append(append([]string{}, batch...), option+"="+shape)...)
			add(ch, "git", append(append([]string{}, batch...), option, shape)...)
		}
	}
	// Window combinations.
	windows := [][]string{
		{"--since", "2026-01-02", "--before", "2026-01-01"}, {"--since", "2026-01-02", "--before", "2026-01-02"}, {"--since", "2026-01-02", "--before", "2026-01-03"},
		{"--since", "2026-01-02", "--backfill", "1"}, {"--since", "2026-01-02", "--backfill", "2"}, {"--backfill", "2", "--since", "2026-01-02"},
		{"--backfill", "1", "--since", "2026-01-02"}, {"--backfill", "01", "--since", "2026-01-02"}, {"--backfill", "+1", "--since", "2026-01-02"},
		{"--backfill", "256", "--since", "2026-01-02"}, {"--backfill", "257", "--since", "2026-01-02"}, {"--backfill", "0", "--since", "2026-01-02"},
		{"--backfill", "3", "--before", "2026-03-10"}, {"--backfill", "3"}, {"--backfill", "400", "--before", "2026-03-10"}, {"--backfill", "-2", "--before", "2026-03-10"},
		{"--day", "2026-03-09", "--backfill", "3"}, {"--date", "2026-03-09", "--backfill", "3"}, {"--day", "2026-03-09", "--date", "2026-04-04", "--backfill", "3"},
		{"--day", "2026-03-09", "--before", "2026-03-20", "--backfill", "3"}, {"--day", "2026-12-31", "--backfill", "2"}, {"--before", "2026-03-01", "--backfill", "2"},
		{"--since", "2026-01-02", "--since", "2026-02-02"}, {"--backfill", "2", "--backfill", "3"}, {"--backfill", "2", "--backfill", "1", "--since", "2026-01-02"},
	}
	for _, window := range windows {
		for _, provider := range [][]string{
			{"--provider", "github", "--owner", "o", "--repo", "r", "--auth", "tok"},
			{"--provider", "gitlab", "--project-id", "3", "--auth", "t"},
			{"--provider", "local"},
		} {
			add(ch, "git", append(append([]string{}, provider...), window...)...)
			add(ch, "blame", append(append([]string{}, provider...), window...)...)
		}
	}

	// Credential-shape rules, one variant at a time.
	credVariants := [][]string{
		{"--auth", "tok"}, {"--auth", ""}, {"--auth", "tok", "--github-app-id", "1"}, {"--github-app-id", "1"}, {"--github-app-key-path", keyFile},
		{"--github-app-installation-id", "9"}, {"--github-app-id", "1", "--github-app-key-path", keyFile}, {"--github-app-id", "1", "--github-app-key-path", keyFile, "--github-app-installation-id", "9"},
		{"--github-app-id", "1", "--github-app-key-path", "/nonexistent/key.pem", "--github-app-installation-id", "9"},
		{"--github-app-id", "", "--github-app-key-path", keyFile, "--github-app-installation-id", "9"},
		{"--github-app-id", "1", "--github-app-key-path", keyFile, "--github-app-installation-id", "9", "--auth", "tok"},
		{"--github-app-key-path", "", "--auth", "tok"}, {"--github-app-id", "", "--auth", "tok"},
	}
	credEnvs := []map[string]string{ch, with(ch, "GITHUB_TOKEN", "envtok"), with(ch, "GITHUB_APP_ID", "5", "GITHUB_APP_PRIVATE_KEY_PATH", keyFile, "GITHUB_APP_INSTALLATION_ID", "6"),
		with(ch, "GITHUB_APP_ID", "5"), with(ch, "GITHUB_TOKEN", "envtok", "GITHUB_APP_INSTALLATION_ID", "6"),
		with(ch, "GITHUB_URL", "https://a", "GITHUB_BASE_URL", "https://b", "GITHUB_TOKEN", "t"), with(ch, "GITHUB_URL", "", "GITHUB_BASE_URL", "https://b", "GITHUB_TOKEN", "t"),
		with(ch, "POSTGRES_URI", "postgresql://p", "DATABASE_URI", "postgresql://q"), with(ch, "POSTGRES_URI", "postgresql://p", "ORG_ID", "o1"), with(ch, "DATABASE_URI", "postgresql://q", "ORG_ID", "")}
	for _, cenv := range credEnvs {
		for _, variant := range credVariants {
			add(cenv, "git", append([]string{"--provider", "github", "--owner", "o", "--repo", "r"}, variant...)...)
		}
		add(cenv, "git", "--provider", "github", "--owner", "o", "--repo", "r", "--db", "postgresql://p", "--org", "x")
		add(cenv, "git", "--provider", "github", "--owner", "o", "--repo", "r", "--db", "postgresql://p", "--org", "")
		add(cenv, "git", "--provider", "github", "--owner", "o", "--repo", "r", "--db", "", "--org", "x")
	}
	// Synthetic (sync_synthetic_target): repo-name resolution, the date range
	// and its --since/--before errors, the org requirement and throwaway-
	// database gate of the sync-run-backed targets, --defer-finalize.
	syntheticEnvs := []map[string]string{
		{}, ch, with(ch, "ORG_ID", "envorg"), with(ch, "DEV_HEALTH_ALLOW_SYNTHETIC_SYNC_RUN", "1"),
		with(ch, "ORG_ID", "envorg", "DEV_HEALTH_ALLOW_SYNTHETIC_SYNC_RUN", "1"),
		with(ch, "ORG_ID", "", "DEV_HEALTH_ALLOW_SYNTHETIC_SYNC_RUN", "1"),
		with(ch, "ORG_ID", "envorg", "DEV_HEALTH_ALLOW_SYNTHETIC_SYNC_RUN", "0"),
	}
	syntheticLines := [][]string{
		{}, {"--repo-name", "acme/custom"}, {"--repo-name", ""}, {"--owner", "o", "--repo", "r"}, {"--owner", "o"}, {"--repo", "r"},
		{"--repo-name", "x/y", "--owner", "o", "--repo", "r"}, {"--repo-name", "", "--owner", "o", "--repo", "r"},
		{"-s", "plain/name"}, {"-s", "a*"}, {"-s", "a?b"}, {"-s", ""}, {"--search", "z", "--owner", "o", "--repo", "r"},
		{"--defer-finalize"}, {"--defer-finalize", "--backfill", "3"},
		{"--since", "2026-01-02"}, {"--since", "2026-09-25"}, {"--since", "2026-09-26"}, {"--since", "2026-09-24", "--before", "2026-09-25"},
		{"--since", "2026-01-02", "--before", "2026-01-02"}, {"--since", "2026-01-02", "--before", "2026-01-03"}, {"--since", "2026-03-10", "--before", "2026-03-01"},
		{"--before", "2026-03-01"}, {"--before", "2026-03-01", "--backfill", "3"}, {"--backfill", "7"}, {"--backfill", "0"}, {"--backfill", "-4"}, {"--backfill", "1"},
		{"--day", "2026-03-09"}, {"--date", "2026-01-01", "--before", "2026-02-01"}, {"--day", "2026-03-09", "--backfill", "5"}, {"--day", "2026-03-09", "--since", "2026-03-01"},
		{"--before", "0001-01-01"}, {"--before", "0001-01-02"}, {"--before", "0001-01-01", "--since", "0001-01-01"}, {"--day", "9999-12-31"}, {"--before", "9999-12-31"},
		{"--sink", "mongo"}, {"--sink", "auto"}, {"--sink", "junk"}, {"--analytics-db", " clickhouse://h"}, {"--analytics-db", "clickhouse+native://h"},
		{"--org", "x"}, {"--org", ""}, {"--org", "x", "--defer-finalize"}, {"--db", "postgresql://p"},
		{"--auth", "ignored", "--github-app-id", "1"}, {"--owner", "o", "--repo", "r", "--search", "*"},
	}
	for _, target := range Targets {
		for _, senv := range syntheticEnvs {
			for _, line := range syntheticLines {
				add(senv, target, append([]string{"--provider", "synthetic"}, line...)...)
			}
		}
	}
	return corpus
}

func shapeArgs(shape string) []string {
	if shape == "" {
		return nil
	}
	return []string{"--max-commits-per-repo", shape}
}
