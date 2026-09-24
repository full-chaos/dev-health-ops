package legacyingest

import (
	"math/rand"
	"strings"
)

var (
	listPool   = []string{`[]`, `["a"]`, `["a","b","a"]`, `[1]`, `"a"`, `null`, `[null]`, `{}`, `["é","\ud800"]`}
	reviewPool = []string{`[]`, `null`, `{}`, `"x"`, `[1]`, `[{}]`, `[{"review_id":"r1","reviewer":"u","state":"APPROVED","submitted_at":"2026-01-01T00:00:00Z"}]`,
		`[{"review_id":1,"reviewer":"u","state":"APPROVED","submitted_at":"2026-01-01T00:00:00Z"}]`, `[{"review_id":"r","reviewer":"u","state":"S","submitted_at":"nope"},{"reviewer":"u"}]`,
		`[{"review_id":"r","reviewer":"u","state":"S","submitted_at":1767225600,"extra":true}]`, `[{"review_id":"r","reviewer":null,"state":"S","submitted_at":null}]`}
	providerPool = []string{`"jira"`, `"github"`, `"gitlab"`, `"linear"`, `"JIRA"`, `"bitbucket"`, `""`, `1`, `null`, `[]`, `true`}
	typePool     = []string{`"story"`, `"task"`, `"bug"`, `"epic"`, `"issue"`, `"incident"`, `"chore"`, `"unknown"`, `"Bug"`, `"feature"`, `null`, `1`, `[]`}
	statusPool   = []string{`"backlog"`, `"todo"`, `"in_progress"`, `"in_review"`, `"blocked"`, `"done"`, `"canceled"`, `"unknown"`, `"open"`, `"Done"`, `null`, `0`, `{}`}
	strPool      = []string{`"x"`, `""`, `"é"`, `"\ud800"`, `"a\u0000b"`, `1`, `1.5`, `true`, `null`, `[]`, `{}`, `"😀"`, `"` + strings.Repeat("y", 300) + `"`}
	dtPool       = []string{`"2026-01-01T00:00:00Z"`, `"2026-01-01T00:00:00"`, `"2026-01-01"`, `"2026-01-01T00:00:00.123456+02:00"`, `"2026-01-01 00:00:00-05:30"`,
		`1767225600`, `1767225600.5`, `"1767225600"`, `1e20`, `true`, `null`, `""`, `"garbage"`, `[]`, `-1`, `"2026-13-01"`, `"2026-01-01T25:00:00Z"`, `{}`}
	intPool   = []string{`1`, `0`, `-1`, `"5"`, `5.0`, `5.5`, `true`, `null`, `"x"`, `1e3`, `9223372036854775808`, `"٣"`, `[]`, `"_1"`, `" 7 "`}
	floatPool = []string{`0.5`, `1`, `"0.5"`, `"nan"`, `1e999`, `"inf"`, `true`, `null`, `[]`, `-0.0`, `"1e5"`, `2.5e-7`, `100000000000000000000.0`, `1e16`,
		// A 309-digit integer literal: its nearest float64 is +Inf, and
		// pydantic-core refuses it as float_type rather than storing inf
		// (CHAOS-6491's shared PydanticFloat fix; distinct from the 1e999
		// exponent-form case above, which json.loads/jiter already parse as
		// inf before pydantic ever sees a value).
		strings.Repeat("9", 309)}
)

type modelField struct {
	name string
	pool []string
	ok   string // a valid value
}

// models are the three request models this area serves: their item fields.
var models = map[string][]modelField{
	"commits": {
		{"hash", strPool, `"abc"`}, {"message", strPool, `"m"`}, {"author_name", strPool, `"a"`}, {"author_email", strPool, `"a@x"`},
		{"author_when", dtPool, `"2026-01-01T00:00:00Z"`}, {"committer_name", strPool, `"c"`}, {"committer_email", strPool, `"c@x"`},
		{"committer_when", dtPool, `"2026-01-02T00:00:00+01:00"`}, {"parents", intPool, `2`},
	},
	"deployments": {
		{"deployment_id", strPool, `"d1"`}, {"status", strPool, `"success"`}, {"environment", strPool, `"prod"`},
		{"started_at", dtPool, `"2026-01-01T00:00:00Z"`}, {"finished_at", dtPool, `"2026-01-01T01:00:00Z"`}, {"deployed_at", dtPool, `"2026-01-01T00:30:00.5Z"`},
		{"pull_request_number", intPool, `7`}, {"release_ref", strPool, `"v1"`}, {"release_ref_confidence", floatPool, `0.75`},
	},
	"pull-requests": {
		{"number", intPool, `12`}, {"title", strPool, `"t"`}, {"body", strPool, `"b"`}, {"state", strPool, `"open"`}, {"author_name", strPool, `"a"`},
		{"author_email", strPool, `"a@x"`}, {"created_at", dtPool, `"2026-01-01T00:00:00Z"`}, {"merged_at", dtPool, `"2026-01-02T00:00:00Z"`},
		{"closed_at", dtPool, `"2026-01-03T00:00:00+02:00"`}, {"head_branch", strPool, `"feat"`}, {"base_branch", strPool, `"main"`},
		{"additions", intPool, `3`}, {"deletions", intPool, `4`}, {"changed_files", intPool, `5`}, {"reviews", reviewPool, `[{"review_id":"r1","reviewer":"u","state":"APPROVED","submitted_at":"2026-01-01T00:00:00Z"}]`},
	},
	"work-items": {
		{"work_item_id", strPool, `"jira:ABC-1"`}, {"provider", providerPool, `"jira"`}, {"title", strPool, `"t"`}, {"type", typePool, `"bug"`}, {"status", statusPool, `"done"`},
		{"status_raw", strPool, `"Done"`}, {"description", strPool, `"d"`}, {"project_key", strPool, `"ABC"`}, {"assignees", listPool, `["a","b"]`}, {"reporter", strPool, `"r"`},
		{"created_at", dtPool, `"2026-01-01T00:00:00Z"`}, {"updated_at", dtPool, `"2026-01-02T00:00:00Z"`}, {"started_at", dtPool, `"2026-01-01T01:00:00Z"`},
		{"completed_at", dtPool, `"2026-01-03T00:00:00Z"`}, {"labels", listPool, `["x"]`}, {"story_points", floatPool, `2.5`}, {"priority_raw", strPool, `"P1"`}, {"url", strPool, `"https://x"`},
	},
	"incidents": {
		{"incident_id", strPool, `"i1"`}, {"status", strPool, `"open"`}, {"started_at", dtPool, `"2026-01-01T00:00:00Z"`}, {"resolved_at", dtPool, `"2026-01-01T02:00:00Z"`},
	},
}

var rootPool = map[string][]string{"org_id": strPool, "repo_url": strPool}

// requiredFields are the item fields with no default.
var requiredFields = map[string][]string{
	"commits":       {"hash", "message", "author_name", "author_email", "author_when"},
	"deployments":   {"deployment_id", "status", "environment"},
	"incidents":     {"incident_id", "status", "started_at"},
	"pull-requests": {"number", "title", "state", "author_name", "created_at"},
	"work-items":    {"work_item_id", "provider", "title", "created_at"},
}

func itemText(route string, override map[string]string, drop map[string]bool) string {
	var parts []string
	for _, field := range models[route] {
		if drop[field.name] {
			continue
		}
		value := field.ok
		if replaced, ok := override[field.name]; ok {
			value = replaced
		}
		parts = append(parts, `"`+field.name+`":`+value)
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func batchText(route, org, repo, items string) string {
	var parts []string
	if org != "" {
		parts = append(parts, `"org_id":`+org)
	}
	if repo != "" {
		parts = append(parts, `"repo_url":`+repo)
	}
	if items != "" {
		parts = append(parts, `"items":`+items)
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func bodyCorpus() []oracleCase {
	var corpus []oracleCase
	env := map[string]string{"ENVIRONMENT": "dev"}
	add := func(route, body string) { corpus = append(corpus, oracleCase{Route: route, Env: env, Body: body}) }
	for _, route := range []string{"commits", "deployments", "incidents", "pull-requests", "work-items"} {
		good := itemText(route, nil, nil)
		add(route, batchText(route, `"o"`, `"r"`, "["+good+"]"))
		for _, body := range []string{``, `null`, `[]`, `1`, `"x"`, `{}`, `{`, `{"a":1}`, `{"org_id":1,"repo_url":2,"items":3}`,
			batchText(route, `"o"`, `"r"`, `[]`), batchText(route, `"o"`, `"r"`, `null`), batchText(route, `"o"`, `"r"`, `{}`), batchText(route, `"o"`, `"r"`, `"x"`),
			batchText(route, `"o"`, `"r"`, `[1,"x",null,[],{}]`), batchText(route, `"o"`, ``, "["+good+"]"), batchText(route, ``, `"r"`, "["+good+"]"),
			batchText(route, `"o"`, `"r"`, "["+good+"]") + ` x`, `{"org_id":"a","org_id":"b","repo_url":"r","items":[` + good + `]}`,
			`{"org_id":"o","repo_url":"r","items":[` + good + `],"extra":{"a":[1]}}`,
			`{"org_id":"o","repo_url":"r","items":[{}]}`, `{"org_id":"o","repo_url":"r","items":[` + strings.Repeat(good+",", 2) + good + `]}`,
			`{"org_id":"o` + strings.Repeat("0", 4300) + `":1}`, `{"org_id":"o","repo_url":"r","items":[` + strings.Replace(good, `"`+models[route][0].name+`":`+models[route][0].ok, `"`+models[route][0].name+`":`+strings.Repeat("1", 4301), 1) + `]}`,
		} {
			add(route, body)
		}
		for _, size := range []int{999, 1000, 1001, 1200} {
			items := make([]string, size)
			for index := range items {
				items[index] = good
			}
			add(route, batchText(route, `"o"`, `"r"`, "["+strings.Join(items, ",")+"]"))
		}
		// 1001 items, some invalid: the length error dominates the item errors.
		bad := itemText(route, map[string]string{models[route][0].name: `null`}, nil)
		many := make([]string, 1001)
		for index := range many {
			many[index] = good
		}
		many[3] = bad
		add(route, batchText(route, `"o"`, `"r"`, "["+strings.Join(many, ",")+"]"))
		for name, pool := range rootPool {
			for _, value := range pool {
				org, repo := `"o"`, `"r"`
				if name == "org_id" {
					org = value
				} else {
					repo = value
				}
				add(route, batchText(route, org, repo, "["+good+"]"))
			}
		}
		required := map[string]bool{}
		for _, name := range requiredFields[route] {
			required[name] = true
		}
		for _, field := range models[route] {
			for _, value := range field.pool {
				add(route, batchText(route, `"o"`, `"r"`, "["+itemText(route, map[string]string{field.name: value}, nil)+"]"))
			}
			add(route, batchText(route, `"o"`, `"r"`, "["+itemText(route, nil, map[string]bool{field.name: true})+"]"))
		}
		random := rand.New(rand.NewSource(int64(len(route)) * 6249))
		for range 700 {
			override := map[string]string{}
			drop := map[string]bool{}
			for _, field := range models[route] {
				switch random.Intn(5) {
				case 0:
					drop[field.name] = true
				case 1:
					override[field.name] = field.pool[random.Intn(len(field.pool))]
				}
			}
			count := 1 + random.Intn(3)
			items := make([]string, count)
			for index := range items {
				items[index] = itemText(route, override, drop)
				if random.Intn(3) == 0 {
					items[index] = good
				}
			}
			org, repo := `"o"`, `"r"`
			if random.Intn(8) == 0 {
				org = strPool[random.Intn(len(strPool))]
			}
			if random.Intn(8) == 0 {
				repo = strPool[random.Intn(len(strPool))]
			}
			add(route, batchText(route, org, repo, "["+strings.Join(items, ",")+"]"))
		}
	}
	return corpus
}
