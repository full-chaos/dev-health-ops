package reviewedges

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/datahealth"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// People of a review edge (CHAOS-8485).
//
// review_edges_daily stores each person as ONE string: the reviewer is the
// review's login or display name; the author is the pull request's
// author_email when there is one, else its author_name, else "unknown"
// (internal/jobs/metrics/daily/reviewedges/compute.go, NormalizeGitIdentity).
// So most author strings are e-mail addresses, and a caller that may not show
// an e-mail address had nothing else to show.
//
// Each row now also serves, for the reviewer and for the author:
//
//   - a NAME (nullable): the display name of the org's identity the stored
//     string belongs to; else, for an author stored by e-mail, the author name
//     the provider gave on the pull request; else the stored string itself
//     when it is not an e-mail address (a provider login). Never an e-mail
//     address. Null means that no name is known: it is not filled with a part
//     of the address.
//   - a KEY (never null): an opaque value that is the same for the same person
//     in every answer of the org, as reviewer and as author. It is a digest,
//     so it is not an e-mail address and not a name.
//
// Two reads at most per answer, whatever the number of rows: the org's
// identities, and (only when an author stored by e-mail has no name yet) the
// pull request author names of those e-mail addresses. A failed read fails
// the request: a name that could not be read is not the same as "no name
// known" (CHAOS-8186).

// identitiesSQL reads every active identity of the org. provider_identities is
// JSON text, so the match is made in Go (datahealth.IdentityKey), not in SQL.
const identitiesSQL = `
SELECT toString(identity_uuid) AS identity_uuid,
       canonical_id,
       ifNull(email, '') AS email,
       ifNull(display_name, '') AS display_name,
       provider_identities
FROM identities FINAL
WHERE org_id = {org_id:String} AND is_active = 1`

// pullRequestAuthorNamesSQL gives, per author e-mail address, the author name
// of the most recent pull request that carries one. argMax over the tuple
// (created_at, name) is deterministic when two pull requests share a
// created_at.
//
// The inner name column is pr_author_name, NOT name: ClickHouse reads a name in
// WHERE as the SELECT alias of the same name, so "argMax(name, ...) AS name"
// with "WHERE name != ”" is "an aggregate function in WHERE" (code 184) on a
// real engine. A fake client cannot see that; the integration test does.
const pullRequestAuthorNamesSQL = `
SELECT email, argMax(pr_author_name, tuple(created_at, pr_author_name)) AS name
FROM (
    SELECT lowerUTF8(trimBoth(ifNull(author_email, ''))) AS email,
           trimBoth(ifNull(author_name, '')) AS pr_author_name,
           created_at
    FROM git_pull_requests FINAL
    WHERE org_id = {org_id:String}
      AND lowerUTF8(trimBoth(ifNull(author_email, ''))) IN {emails:Array(String)}
)
WHERE pr_author_name != ''
GROUP BY email`

// unknownIdentity is the string the writer stores when a pull request has
// neither an author e-mail nor an author name.
const unknownIdentity = "unknown"

// identity is one identity row of the org.
type identity struct {
	uuid        string
	displayName string
}

// identityIndex finds the identity a stored string belongs to.
type identityIndex struct {
	byKey     map[string]*identity
	ambiguous map[string]struct{}
}

func (index *identityIndex) add(value string, row *identity) {
	key := datahealth.IdentityKey(value)
	if key == "" {
		return
	}
	if existing, ok := index.byKey[key]; ok && existing.uuid != row.uuid {
		// Two identities share this value (two people with one display name).
		// The value then names neither: a wrong name is worse than no name.
		index.ambiguous[key] = struct{}{}
		return
	}
	index.byKey[key] = row
}

func (index *identityIndex) resolve(stored string) *identity {
	key := datahealth.IdentityKey(stored)
	if key == "" {
		return nil
	}
	if _, ok := index.ambiguous[key]; ok {
		return nil
	}
	return index.byKey[key]
}

func loadIdentityIndex(ctx context.Context, client QueryClient, orgID string) (*identityIndex, error) {
	rows, err := client.Query(ctx, identitiesSQL, []clickhouse.Binding{{Name: "org_id", Value: orgID}})
	if err != nil {
		return nil, fmt.Errorf("reviewedges: identities query: %w", err)
	}
	defer rows.Close()

	index := &identityIndex{byKey: map[string]*identity{}, ambiguous: map[string]struct{}{}}
	for rows.Next() {
		var uuid, canonicalID, email, displayName, providerIdentities string
		if scanErr := rows.Scan(&uuid, &canonicalID, &email, &displayName, &providerIdentities); scanErr != nil {
			return nil, fmt.Errorf("reviewedges: identities scan: %w", scanErr)
		}
		row := &identity{uuid: uuid, displayName: strings.TrimSpace(displayName)}
		for _, value := range []string{canonicalID, email, displayName} {
			index.add(value, row)
		}
		for _, value := range datahealth.ProviderIdentityValues(providerIdentities) {
			index.add(value, row)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("reviewedges: identities rows: %w", err)
	}
	return index, nil
}

func loadPullRequestAuthorNames(ctx context.Context, client QueryClient, orgID string, emails []string) (map[string]string, error) {
	names := map[string]string{}
	if len(emails) == 0 {
		return names, nil
	}
	rows, err := client.Query(ctx, pullRequestAuthorNamesSQL, []clickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "emails", Value: emails},
	})
	if err != nil {
		return nil, fmt.Errorf("reviewedges: pull request author names query: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var email, name string
		if scanErr := rows.Scan(&email, &name); scanErr != nil {
			return nil, fmt.Errorf("reviewedges: pull request author names scan: %w", scanErr)
		}
		names[email] = name
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("reviewedges: pull request author names rows: %w", err)
	}
	return names, nil
}

// isEmailAddress is true for every string that holds an "@". It is wider than
// a real address on purpose: a value that could be an address is never served
// as a name.
func isEmailAddress(value string) bool { return strings.Contains(value, "@") }

// emailKey is the form an author e-mail address has in pullRequestAuthorNamesSQL.
func emailKey(value string) string { return strings.ToLower(strings.TrimSpace(value)) }

// personName is the name served for a stored identity, by the rule in this
// file's header. Nil = no name known.
func personName(stored string, resolved *identity, pullRequestAuthorName string) *string {
	candidates := make([]string, 0, 3)
	if resolved != nil {
		candidates = append(candidates, resolved.displayName)
	}
	candidates = append(candidates, strings.TrimSpace(pullRequestAuthorName))
	if trimmed := strings.TrimSpace(stored); !strings.EqualFold(trimmed, unknownIdentity) {
		candidates = append(candidates, trimmed)
	}
	for _, candidate := range candidates {
		if candidate == "" || isEmailAddress(candidate) {
			continue
		}
		name := candidate
		return &name
	}
	return nil
}

// personKey is the opaque key of a person inside the org: a digest of the
// identity when the stored string belongs to one, else of the stored string.
// So the same person has one key as reviewer and as author when the identity
// resolves, and an unresolved string still has a key of its own.
func personKey(orgID, stored string, resolved *identity) string {
	basis := "raw\x00" + datahealth.IdentityKey(stored)
	if resolved != nil {
		basis = "identity\x00" + resolved.uuid
	}
	sum := sha256.Sum256([]byte(orgID + "\x00" + basis))
	return "p_" + hex.EncodeToString(sum[:10])
}

// attachPeople fills the names and the keys of every edge.
func attachPeople(ctx context.Context, client QueryClient, orgID string, edges []model.ReviewEdgeRow) error {
	if len(edges) == 0 {
		return nil
	}
	index, err := loadIdentityIndex(ctx, client, orgID)
	if err != nil {
		return err
	}

	// Authors stored by e-mail whose identity gives no name: ask the pull
	// requests, once, for all of them.
	wanted := map[string]struct{}{}
	for _, edge := range edges {
		if !isEmailAddress(edge.Author) {
			continue
		}
		if resolved := index.resolve(edge.Author); resolved != nil && resolved.displayName != "" && !isEmailAddress(resolved.displayName) {
			continue
		}
		wanted[emailKey(edge.Author)] = struct{}{}
	}
	emails := make([]string, 0, len(wanted))
	for email := range wanted {
		emails = append(emails, email)
	}
	sort.Strings(emails)
	authorNames, err := loadPullRequestAuthorNames(ctx, client, orgID, emails)
	if err != nil {
		return err
	}

	for i, edge := range edges {
		reviewer := index.resolve(edge.Reviewer)
		author := index.resolve(edge.Author)
		// The reviewer string is a login or a display name, never the e-mail
		// of a pull request author: no pull request name is asked for it.
		// The row is written again whole, as one literal: the registered-document gate
		// (TestRegisteredDocumentFieldsArePopulatable) reads a field as served only when a
		// literal sets it. Every field the row already had is carried over.
		edges[i] = model.ReviewEdgeRow{
			Reviewer:     edge.Reviewer,
			Author:       edge.Author,
			ReviewsCount: edge.ReviewsCount,
			Day:          edge.Day,
			RepoID:       edge.RepoID,
			ReviewerName: personName(edge.Reviewer, reviewer, ""),
			AuthorName:   personName(edge.Author, author, authorNames[emailKey(edge.Author)]),
			ReviewerKey:  personKey(orgID, edge.Reviewer, reviewer),
			AuthorKey:    personKey(orgID, edge.Author, author),
		}
	}
	return nil
}
