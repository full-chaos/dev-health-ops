package fixturesgen

import (
	"math/big"
	"strconv"

	"github.com/google/uuid"
)

// This file ports providers/teams.py's `sync_teams` synthetic branch
// (CHAOS-7037): `SyntheticDataGenerator().generate_teams(count=8)`, which
// resolves to fixtures/generators/base.py's `get_team_assignment` over the
// SAME __init__-seeded RNG fixtures/generator.py:44-62 seeds unconditionally.
//
// The `sync teams --provider synthetic` CLI verb (providers/teams.py:
// register_commands) has NO --repo-name/--seed flags -- it always
// constructs `SyntheticDataGenerator()` with zero arguments, so this port's
// only live configuration is `count` (fixed at 8 by the CLI call site,
// providers/teams.py:574). With repo_name and seed both fixed, this whole
// generation is DETERMINISTIC: the same 8 teams every run, forever, unless
// DemoAuthors/DemoTeams/DefaultDemoRepoName themselves change on the Python
// side -- which the live-python-oracle (teams_oracle_test.go) catches.

// demoNamespace is uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
// (fixtures/generator.py:58, also demo_identity.py's user/org uuid5
// namespace) -- NOT teamsidentity.go's namespaceURL
// (6ba7b811-...), which is a different, unrelated RFC 4122 constant for a
// different purpose (that one keys team_uuid/identity_uuid; this one keys
// the synthetic repo_id the RNG seed is derived from).
var demoNamespace = uuid.MustParse("6ba7b810-9dad-11d1-80b4-00c04fd430c8")

// DefaultDemoRepoName is demo_identity.py's DEFAULT_DEMO_REPO_NAME
// (DEMO_REPO_NAMES[0]) -- the repo_name SyntheticDataGenerator() defaults to
// when the caller (here, and providers/teams.py's synthetic branch) passes
// none.
const DefaultDemoRepoName = "meridian/web-app"

// DemoAuthors is fixtures/generator.py:63-80's 16 (name, email) pairs, in
// their EXACT literal order -- get_team_assignment's chunking depends on the
// order random.shuffle leaves them in, which depends on this starting order
// byte-for-byte.
var DemoAuthors = [16][2]string{
	{"Alice Smith", "alice@example.com"},
	{"Bob Jones", "bob@example.com"},
	{"Charlie Brown", "charlie@example.com"},
	{"David White", "david@example.com"},
	{"Eve Black", "eve@example.com"},
	{"Frank Green", "frank@example.com"},
	{"Grace Hall", "grace@example.com"},
	{"Heidi Blue", "heidi@example.com"},
	{"Ivan Red", "ivan@example.com"},
	{"Judy Orange", "judy@example.com"},
	{"Kevin Purple", "kevin@example.com"},
	{"Liam Cyan", "liam@example.com"},
	{"Mia Magenta", "mia@example.com"},
	{"Noah Yellow", "noah@example.com"},
	{"Olivia Gray", "olivia@example.com"},
	{"Pat Lime", "pat@example.com"},
}

// DemoTeams is demo_identity.py's DEMO_TEAMS: the curated (team_id,
// team_name) pairs get_team_assignment (base.py:81-121) draws on before
// falling back to the legacy "team-{n}"/"Team {n}" scheme past index 9.
// count=8 (the only count the CLI verb ever requests) never reaches that
// fallback.
var DemoTeams = [10][2]string{
	{"core", "Core"},
	{"platform", "Platform"},
	{"growth", "Growth"},
	{"data", "Data"},
	{"infra", "Infra"},
	{"mobile", "Mobile"},
	{"reliability", "Reliability"},
	{"research", "Research"},
	{"security", "Security"},
	{"docs", "Docs"},
}

// SyntheticTeam is one row of get_team_assignment's "teams" list: only the
// fields generate_teams' caller (providers/teams.py) actually reads --
// id/name/members. The generator itself never sets description (Team's
// description defaults to None/nil) or manual_members.
type SyntheticTeam struct {
	ID      string
	Name    string
	Members []string
}

// seedFromRepoName is fixtures/generator.py:56-61's no-seed branch:
// repo_id = uuid5(demoNamespace, repo_name); seed = repo_id.int % 2**32.
// uuid.UUID.int in Python is the 128-bit value read big-endian; Go's
// uuid.UUID is already that same 16-byte big-endian array, so
// big.Int.SetBytes(id[:]) is the identical integer before the mod.
func seedFromRepoName(repoName string) *big.Int {
	repoID := uuid.NewSHA1(demoNamespace, []byte(repoName))
	asInt := new(big.Int).SetBytes(repoID[:])
	mod := new(big.Int).Lsh(big.NewInt(1), 32) // 2**32
	return asInt.Mod(asInt, mod)
}

// GenerateSyntheticTeams ports get_team_assignment(count) as reached by
// generate_teams via SyntheticDataGenerator's __init__ + shuffle (base.py:
// 81-121, generator.py:44-82). repoName "" and seed nil both take the same
// defaults the zero-argument `SyntheticDataGenerator()` call in providers/
// teams.py:573 takes: repoName -> DefaultDemoRepoName, seed ->
// seedFromRepoName(repoName). count must be > 0 (the CLI always passes 8;
// Python's own chunk_size = max(1, len(authors)//count) never raises for a
// larger count, so this does not either -- see the doc comment on the
// count > len(authors) shape below).
func GenerateSyntheticTeams(repoName string, seed *big.Int, count int) []SyntheticTeam {
	if repoName == "" {
		repoName = DefaultDemoRepoName
	}
	if seed == nil {
		seed = seedFromRepoName(repoName)
	}
	if count <= 0 {
		return nil
	}

	rng := NewRand(seed)
	authors := DemoAuthors // array value: a copy, shuffle never mutates the package var
	rng.Shuffle(len(authors), func(i, j int) { authors[i], authors[j] = authors[j], authors[i] })

	// chunk_size = max(1, len(authors)//count) (base.py:91); a count larger
	// than len(authors) makes chunk_size=1 and the LAST team's slice (start:
	// len(authors)) empty once start>=len(authors) -- Python's own slicing
	// silently returns [] past the end too, so this mirrors it rather than
	// guarding against it.
	chunkSize := len(authors) / count
	if chunkSize < 1 {
		chunkSize = 1
	}

	teams := make([]SyntheticTeam, 0, count)
	for i := 0; i < count; i++ {
		start := i * chunkSize
		end := (i + 1) * chunkSize
		if i == count-1 || end > len(authors) {
			end = len(authors)
		}
		if start > len(authors) {
			start = len(authors)
		}
		chunk := authors[start:end]

		var id, name string
		if i < len(DemoTeams) {
			id, name = DemoTeams[i][0], DemoTeams[i][1]
		} else {
			id, name = legacyTeamID(i), legacyTeamName(i)
		}

		members := make([]string, len(chunk))
		for k, author := range chunk {
			members[k] = author[1]
		}
		teams = append(teams, SyntheticTeam{ID: id, Name: name, Members: members})
	}
	return teams
}

// legacyTeamID/legacyTeamName are base.py:106-107's fallback once DemoTeams
// (demo_team_identity) is exhausted: f"team-{i+1}" / f"Team {i+1}". count=8
// (the only count the CLI verb requests) never reaches this -- DemoTeams has
// 10 entries -- but GenerateSyntheticTeams accepts a larger count too, so
// this stays a real branch, not dead code.
func legacyTeamID(i int) string   { return "team-" + strconv.Itoa(i+1) }
func legacyTeamName(i int) string { return "Team " + strconv.Itoa(i+1) }
