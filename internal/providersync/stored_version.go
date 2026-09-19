package providersync

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/storedversion"
)

// Stable log event names for provider-sync columns kept by a contract table
// outside the named guards below.
const (
	storedVersionCarriedEvent = "providersync.stored_version.carried_forward"
	storedVersionRefusedEvent = "providersync.stored_version.null_over_value_refused"
)

// pullRequestReviewRegressionRefusedEvent names a successful review
// enrichment that stated an empty review list over a held first review; the
// held first_review_at and both counts are kept as one unit.
const pullRequestReviewRegressionRefusedEvent = "providersync.pull_request.review_regression_refused"

// reviewsLookupField is the source field of the review-derived pull request
// columns: unstated for a row whose review enrichment failed this pass.
const reviewsLookupField = "reviews_lookup"

// org_id is last, mirroring _insert_rows' auto-injection order in
// storage/clickhouse.py (`columns = [*columns, "org_id"]`).
const pullRequestInsert = `
INSERT INTO git_pull_requests (
  repo_id, number, title, body, state, author_name, author_email,
  created_at, merged_at, closed_at, head_branch, base_branch,
  additions, deletions, changed_files, first_review_at, first_comment_at,
  changes_requested_count, reviews_count, comments_count, last_synced,
  source_id, org_id
)`

const pullRequestReviewInsert = `
INSERT INTO git_pull_request_reviews (
  repo_id, number, review_id, reviewer, state, submitted_at,
  last_synced, source_id, org_id
)`

func contractColumns(rule storedversion.Rule, names ...string) []storedversion.Column {
	columns := make([]storedversion.Column, 0, len(names))
	for _, name := range names {
		columns = append(columns, storedversion.Column{Name: name, Rule: rule, Fields: []string{name}})
	}
	return columns
}

func joinColumns(groups ...[]storedversion.Column) []storedversion.Column {
	var out []storedversion.Column
	for _, group := range groups {
		out = append(out, group...)
	}
	return out
}

// pullRequestContract is the provider-sync writer's contract table for
// git_pull_requests (github PR-social datasets and gitlab prs share the
// writer). The provider fetch states every column except first_comment_at,
// which no provider pass produces, and the review-derived columns, which are
// unstated when the review enrichment failed. merged_at and first_review_at
// record events that cannot un-happen. The review counts are kept together
// with first_review_at: an enrichment that states an empty review list over a
// held first review keeps all three, so the row never says "first review at
// T, zero reviews".
var pullRequestContract = storedversion.Contract{Writer: "provider_sync", Table: "git_pull_requests", Columns: joinColumns(
	contractColumns(storedversion.Identity, "repo_id", "number"),
	contractColumns(storedversion.Stated, "title", "body", "state", "author_name", "author_email", "created_at"),
	[]storedversion.Column{
		{Name: "merged_at", Rule: storedversion.Stated, Fields: []string{"merged_at"}, Terminal: true},
		{Name: "closed_at", Rule: storedversion.StateCoupled, Fields: []string{"closed_at"}},
	},
	contractColumns(storedversion.Stated, "head_branch", "base_branch", "additions", "deletions", "changed_files"),
	[]storedversion.Column{
		{Name: "first_review_at", Rule: storedversion.Unstated, Fields: []string{reviewsLookupField}, Terminal: true},
		{Name: "first_comment_at", Rule: storedversion.NoField},
		{Name: "changes_requested_count", Rule: storedversion.Unstated, Fields: []string{reviewsLookupField}, With: "first_review_at"},
		{Name: "reviews_count", Rule: storedversion.Unstated, Fields: []string{reviewsLookupField}, With: "first_review_at"},
	},
	contractColumns(storedversion.Stated, "comments_count"),
	contractColumns(storedversion.Writer, "last_synced", "source_id", "org_id"),
)}

// pullRequestReviewContract: a review row states every column.
var pullRequestReviewContract = storedversion.Contract{Writer: "provider_sync", Table: "git_pull_request_reviews", Columns: joinColumns(
	contractColumns(storedversion.Identity, "repo_id", "number", "review_id"),
	contractColumns(storedversion.Stated, "reviewer", "state", "submitted_at"),
	contractColumns(storedversion.Writer, "last_synced", "source_id", "org_id"),
)}

func pullRequestCarry(row pullRequestRow) map[string]bool {
	return pullRequestContract.Carry(func(field string) bool {
		return field == reviewsLookupField && row.ReviewsLookupFailed
	})
}

// nullable turns a nil pointer into an untyped nil, the form a contract reads
// as a stated null.
func nullableTimePointer(value *time.Time) any {
	if value == nil {
		return nil
	}
	return *value
}

func nullableStringPointer(value *string) any {
	if value == nil {
		return nil
	}
	return *value
}

func pullRequestValues(row pullRequestRow) []any {
	return []any{
		row.RepoID, row.Number, nullableStringPointer(row.Title), nullableStringPointer(row.Body), row.State,
		row.AuthorName, nullableStringPointer(row.AuthorEmail), row.CreatedAt, nullableTimePointer(row.MergedAt),
		nullableTimePointer(row.ClosedAt), nullableStringPointer(row.HeadBranch), nullableStringPointer(row.BaseBranch),
		row.Additions, row.Deletions, row.ChangedFiles, nullableTimePointer(row.FirstReviewAt),
		nullableTimePointer(row.FirstCommentAt), row.ChangesRequestedCount, row.ReviewsCount,
		row.CommentsCount, row.LastSynced, nullableStringPointer(row.SourceID), row.OrgID,
	}
}

func heldTime(value any) (*time.Time, error) {
	switch typed := value.(type) {
	case nil:
		return nil, nil
	case time.Time:
		return &typed, nil
	}
	return nil, fmt.Errorf("stored version: %T is not a timestamp", value)
}

func heldCount(value any) (int, error) {
	switch typed := value.(type) {
	case int:
		return typed, nil
	case uint32:
		return int(typed), nil
	}
	return 0, fmt.Errorf("stored version: %T is not a count", value)
}

// setPullRequestKept writes a kept column's value back into the typed row.
func setPullRequestKept(row *pullRequestRow, column string, value any) error {
	var err error
	switch column {
	case "merged_at":
		row.MergedAt, err = heldTime(value)
	case "first_review_at":
		row.FirstReviewAt, err = heldTime(value)
	case "first_comment_at":
		row.FirstCommentAt, err = heldTime(value)
	case "changes_requested_count":
		row.ChangesRequestedCount, err = heldCount(value)
	case "reviews_count":
		row.ReviewsCount, err = heldCount(value)
	default:
		err = fmt.Errorf("stored version: git_pull_requests column %s is not kept", column)
	}
	return err
}

// applyPullRequestContract reads the held version of every row's key and
// rewrites the rows in place as the contract says. WriteEffect and the
// recovery readback's expected rows both go through it, so a guarded write
// reads back exact. A failed read fails the caller.
func applyPullRequestContract(
	ctx context.Context, conn storedversion.Querier, claim Claim, rows []pullRequestRow, log bool,
) error {
	stored := make([]storedversion.Row, len(rows))
	for i, row := range rows {
		stored[i] = storedversion.Row{Values: pullRequestValues(row), Carry: pullRequestCarry(row)}
	}
	outcomes, err := pullRequestContract.Apply(ctx, conn, claim.OrgID, pullRequestInsert, stored)
	if err != nil {
		return err
	}
	positions, err := storedversion.Positions(pullRequestInsert)
	if err != nil {
		return err
	}
	for i := range rows {
		for _, column := range pullRequestContract.Kept() {
			if err := setPullRequestKept(&rows[i], column, stored[i].Values[positions[column]]); err != nil {
				return err
			}
		}
	}
	if log {
		logPullRequestOutcomes(ctx, claim, rows, outcomes)
	}
	return nil
}

// logPullRequestOutcomes keeps the named guard events: a refused null
// merged_at, and review columns carried over a failed enrichment. Every
// other kept column logs under the stored-version events.
func logPullRequestOutcomes(ctx context.Context, claim Claim, rows []pullRequestRow, outcomes []storedversion.Outcome) {
	var mergedAt []pullRequestMergedAtRegressionGuarded
	var reviews []pullRequestReviewsCarriedForward
	rest := make([]storedversion.Outcome, 0, len(outcomes))
	for i, outcome := range outcomes {
		other := storedversion.Outcome{Key: outcome.Key}
		reviewUnitRefused := false
		for _, column := range outcome.Refused {
			if column == "merged_at" && rows[i].MergedAt != nil {
				mergedAt = append(mergedAt, pullRequestMergedAtRegressionGuarded{
					RepoID: rows[i].RepoID, Number: rows[i].Number, StoredMergedAt: *rows[i].MergedAt,
				})
				continue
			}
			if column == "first_review_at" || column == "changes_requested_count" || column == "reviews_count" {
				reviewUnitRefused = true
				continue
			}
			other.Refused = append(other.Refused, column)
		}
		if reviewUnitRefused && rows[i].FirstReviewAt != nil {
			slog.Default().LogAttrs(ctx, slog.LevelWarn, pullRequestReviewRegressionRefusedEvent,
				slog.String("org_id", claim.OrgID), slog.String("provider", claim.Provider),
				slog.String("dataset", claim.Dataset), slog.String("unit_id", claim.ID),
				slog.String("repo_id", rows[i].RepoID), slog.Int("number", rows[i].Number),
				slog.Time("stored_first_review_at", rows[i].FirstReviewAt.UTC()),
				slog.Int("stored_reviews_count", rows[i].ReviewsCount),
				slog.Int("stored_changes_requested_count", rows[i].ChangesRequestedCount),
			)
		}
		carriedReviews := false
		for _, column := range outcome.Carried {
			if rows[i].ReviewsLookupFailed && (column == "first_review_at" || column == "changes_requested_count" || column == "reviews_count") {
				carriedReviews = true
				continue
			}
			other.Carried = append(other.Carried, column)
		}
		if carriedReviews && (rows[i].FirstReviewAt != nil || rows[i].ReviewsCount != 0 || rows[i].ChangesRequestedCount != 0) {
			reviews = append(reviews, pullRequestReviewsCarriedForward{RepoID: rows[i].RepoID, Number: rows[i].Number})
		}
		rest = append(rest, other)
	}
	logPullRequestMergedAtRegressionGuarded(ctx, claim, mergedAt)
	logPullRequestReviewRegressionGuarded(ctx, claim, reviews)
	pullRequestContract.Log(ctx, claim.OrgID, storedVersionCarriedEvent, storedVersionRefusedEvent, rest)
}

// StoredVersionSpecs lists the provider-sync writers under a stored-version
// contract, keyed by table, for the invariant enumeration.
func StoredVersionSpecs() map[string][]storedversion.Spec {
	return map[string][]storedversion.Spec{
		"git_pull_requests": {{
			Name: "provider sync pull requests", Contract: pullRequestContract, Insert: pullRequestInsert,
			Carry: func(payload map[string]any) map[string]bool {
				_, stated := payload[reviewsLookupField]
				return pullRequestCarry(pullRequestRow{ReviewsLookupFailed: !stated})
			},
		}},
		"git_pull_request_reviews": {{
			Name: "provider sync reviews", Contract: pullRequestReviewContract, Insert: pullRequestReviewInsert,
			Carry: func(map[string]any) map[string]bool { return map[string]bool{} },
		}},
		"deployments": {{
			Name: "provider sync deployments", Contract: deploymentsContract, Insert: deploymentsInsert,
			Carry: func(payload map[string]any) map[string]bool {
				_, lifecycle := payload[lifecycleLookupField]
				_, pullRequest := payload[pullRequestLookupField]
				return deploymentCarry(deploymentRow{LifecycleLookupFailed: !lifecycle, PullRequestLookupFailed: !pullRequest})
			},
		}},
		"repos": {{
			Name: "provider sync repositories", Contract: repositoryContract, Insert: repositoryInsert,
			Carry: func(map[string]any) map[string]bool {
				return repositoryContract.Carry(func(string) bool { return false })
			},
		}},
		"git_commits": {{
			Name: "provider sync commits", Contract: gitCommitsContract, Insert: gitCommitsInsert,
			Carry: func(map[string]any) map[string]bool { return map[string]bool{} },
		}},
		"work_items": {
			{
				Name: "provider sync work items", Contract: workItemsContract, Insert: withHeldWorkItemColumns(gitHubWorkItemsInsert),
				Carry: func(map[string]any) map[string]bool {
					return workItemsContract.Carry(func(string) bool { return false })
				},
			},
			{
				Name: "provider sync linear work items", Contract: workItemsContract, Insert: withHeldWorkItemColumns(linearWorkItemsInsert),
				Carry: func(map[string]any) map[string]bool {
					return workItemsContract.Carry(func(string) bool { return false })
				},
			},
		},
	}
}

// workItemsHeldColumns are the work_items columns the provider-sync work-item
// writers never produce a value for; the Python-parity column lists omit
// them, so the writers append them carrying the held value.
var workItemsHeldColumns = []string{"description", "priority_raw", "service_class", "due_at"}

// workItemsContract is the contract table of the provider-sync work-item
// writers (the direct adapter shared by github, gitlab and jira rows, and the
// linear adapter): every column of the Python-parity list is stated as the
// provider row produces it, and the four held columns are R1.
var workItemsContract = storedversion.Contract{Writer: "provider_sync", Table: "work_items", Columns: joinColumns(
	contractColumns(storedversion.Identity, "repo_id", "work_item_id"),
	contractColumns(storedversion.Writer, "provider"),
	contractColumns(storedversion.Stated, "title", "type", "status", "status_raw", "project_key", "project_id",
		"native_team_key", "project_name", "assignees", "reporter", "created_at", "updated_at"),
	contractColumns(storedversion.StateCoupled, "started_at", "completed_at", "closed_at"),
	contractColumns(storedversion.Stated, "labels", "story_points", "sprint_id", "sprint_name", "parent_id", "epic_id", "url"),
	contractColumns(storedversion.Writer, "last_synced", "org_id", "source_id"),
	[]storedversion.Column{
		{Name: "description", Rule: storedversion.NoField}, {Name: "priority_raw", Rule: storedversion.NoField},
		{Name: "service_class", Rule: storedversion.NoField}, {Name: "due_at", Rule: storedversion.NoField},
	},
)}

// withHeldWorkItemColumns appends the held columns to a Python-parity
// work_items insert.
func withHeldWorkItemColumns(insert string) string {
	end := strings.LastIndexByte(insert, ')')
	return insert[:end] + ", " + strings.Join(workItemsHeldColumns, ", ") + insert[end:]
}

// writeWorkItemsKeepingHeldColumns inserts the projected rows with the held
// columns carried from each key's current version. A failed read fails the
// write before any insert.
func writeWorkItemsKeepingHeldColumns(
	ctx context.Context, conn driver.Conn, orgID, insert string, rows []workItemStoredRow,
) error {
	extended := withHeldWorkItemColumns(insert)
	stored := make([]storedversion.Row, len(rows))
	for i, row := range rows {
		values := append(row.values(), make([]any, len(workItemsHeldColumns))...)
		stored[i] = storedversion.Row{Values: values, Carry: workItemsContract.Carry(func(string) bool { return false })}
	}
	outcomes, err := workItemsContract.Apply(ctx, conn, orgID, extended, stored)
	if err != nil {
		return err
	}
	workItemsContract.Log(ctx, orgID, storedVersionCarriedEvent, storedVersionRefusedEvent, outcomes)
	batch, err := conn.PrepareBatch(ctx, extended)
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, row := range stored {
		if err := batch.Append(row.Values...); err != nil {
			return err
		}
	}
	return batch.Send()
}

const repositoryInsert = `
INSERT INTO repos (
  id, org_id, repo, ref, created_at, settings, tags, provider, last_synced
)`

// repositoryContract is the provider-sync repository writer's contract table
// (github and gitlab share it). The provider row never carries a default
// branch, so ref is R1 and keeps the value the key already holds; every other
// column is stated by the provider fetch. source_id is not written: a
// provider version names no external source.
var repositoryContract = storedversion.Contract{Writer: "provider_sync", Table: "repos", Columns: joinColumns(
	contractColumns(storedversion.Identity, "id"),
	contractColumns(storedversion.Writer, "org_id"),
	contractColumns(storedversion.Stated, "repo"),
	[]storedversion.Column{{Name: "ref", Rule: storedversion.NoField}},
	contractColumns(storedversion.Stated, "created_at", "settings", "tags"),
	contractColumns(storedversion.Writer, "provider", "last_synced"),
)}

// applyRepositoryContract reads the held version of every row's key and
// writes the kept ref back into the rows; WriteEffect and the recovery
// readback's expected rows both go through it.
func applyRepositoryContract(ctx context.Context, conn storedversion.Querier, claim Claim, rows []repositoryRow, log bool) error {
	stored := make([]storedversion.Row, len(rows))
	for i, row := range rows {
		stored[i] = storedversion.Row{
			Values: []any{row.ID, row.OrgID, row.Repo, nullableStringPointer(row.Ref), row.CreatedAt, row.Settings, row.Tags, row.Provider, row.LastSynced},
			Carry:  repositoryContract.Carry(func(string) bool { return false }),
		}
	}
	outcomes, err := repositoryContract.Apply(ctx, conn, claim.OrgID, repositoryInsert, stored)
	if err != nil {
		return err
	}
	positions, err := storedversion.Positions(repositoryInsert)
	if err != nil {
		return err
	}
	for i := range rows {
		switch held := stored[i].Values[positions["ref"]].(type) {
		case nil:
			rows[i].Ref = nil
		case string:
			rows[i].Ref = &held
		default:
			return fmt.Errorf("stored version: repos ref %T is not a string", held)
		}
	}
	if log {
		repositoryContract.Log(ctx, claim.OrgID, storedVersionCarriedEvent, storedVersionRefusedEvent, outcomes)
	}
	return nil
}

const gitCommitsInsert = `INSERT INTO git_commits (org_id, repo_id, hash, message, author_name, author_email, author_when, committer_name, committer_email, committer_when, parents, last_synced)`

// gitCommitsContract: a commit is immutable and the provider fetch states
// every column; source_id is not written, as for repositories.
var gitCommitsContract = storedversion.Contract{Writer: "provider_sync", Table: "git_commits", Columns: joinColumns(
	contractColumns(storedversion.Writer, "org_id"),
	contractColumns(storedversion.Identity, "repo_id", "hash"),
	contractColumns(storedversion.Stated, "message", "author_name", "author_email", "author_when",
		"committer_name", "committer_email", "committer_when", "parents"),
	contractColumns(storedversion.Writer, "last_synced"),
)}

// Source fields of the lookup-derived deployment columns: unstated for a row
// whose statuses or per-SHA pull request lookup failed this pass.
const (
	lifecycleLookupField   = "lifecycle_lookup"
	pullRequestLookupField = "pull_request_lookup"
)

const deploymentsInsert = `INSERT INTO deployments (repo_id, deployment_id, status, environment, started_at, finished_at, deployed_at, merged_at, pull_request_number, release_ref, release_ref_confidence, org_id, last_synced)`

// deploymentsContract is the provider-sync deployment writer's contract table
// (github and gitlab share it). The lifecycle columns are unstated when the
// statuses lookup failed and the merged pull request pair when the per-SHA
// lookup failed; every other column is stated. merged_at is terminal and
// pull_request_number is kept together with it: a lookup that succeeds with
// no pull request over a held merge keeps both, since a deployed commit's
// merged pull request cannot un-merge.
var deploymentsContract = storedversion.Contract{Writer: "provider_sync", Table: "deployments", Columns: joinColumns(
	contractColumns(storedversion.Identity, "repo_id", "deployment_id"),
	[]storedversion.Column{{Name: "status", Rule: storedversion.Unstated, Fields: []string{lifecycleLookupField}}},
	contractColumns(storedversion.Stated, "environment"),
	[]storedversion.Column{
		{Name: "started_at", Rule: storedversion.Unstated, Fields: []string{lifecycleLookupField}},
		{Name: "finished_at", Rule: storedversion.Unstated, Fields: []string{lifecycleLookupField}},
	},
	contractColumns(storedversion.Stated, "deployed_at"),
	[]storedversion.Column{
		{Name: "merged_at", Rule: storedversion.Unstated, Fields: []string{pullRequestLookupField}, Terminal: true},
		{Name: "pull_request_number", Rule: storedversion.Unstated, Fields: []string{pullRequestLookupField}, With: "merged_at"},
	},
	contractColumns(storedversion.Stated, "release_ref", "release_ref_confidence"),
	contractColumns(storedversion.Writer, "org_id", "last_synced"),
)}

func deploymentCarry(row deploymentRow) map[string]bool {
	return deploymentsContract.Carry(func(field string) bool {
		return (field == lifecycleLookupField && row.LifecycleLookupFailed) ||
			(field == pullRequestLookupField && row.PullRequestLookupFailed)
	})
}

func deploymentValues(row deploymentRow) []any {
	var number any
	if row.PullRequestNumber != nil {
		number = uint32(*row.PullRequestNumber)
	}
	return []any{
		row.RepoID, row.DeploymentID, nullableStringPointer(row.Status), nullableStringPointer(row.Environment),
		nullableTimePointer(row.StartedAt), nullableTimePointer(row.FinishedAt), nullableTimePointer(row.DeployedAt),
		nullableTimePointer(row.MergedAt), number, row.ReleaseRef, row.ReleaseRefConfidence, row.OrgID, row.LastSynced,
	}
}

// setDeploymentKept writes a kept column's value back into the typed row.
func setDeploymentKept(row *deploymentRow, column string, value any) error {
	var err error
	switch column {
	case "status":
		switch held := value.(type) {
		case nil:
			row.Status = nil
		case string:
			row.Status = &held
		default:
			err = fmt.Errorf("stored version: deployments status %T is not a string", value)
		}
	case "started_at":
		row.StartedAt, err = heldTime(value)
	case "finished_at":
		row.FinishedAt, err = heldTime(value)
	case "merged_at":
		row.MergedAt, err = heldTime(value)
	case "pull_request_number":
		switch held := value.(type) {
		case nil:
			row.PullRequestNumber = nil
		case uint32:
			number := int(held)
			row.PullRequestNumber = &number
		default:
			err = fmt.Errorf("stored version: deployments pull_request_number %T is not a count", value)
		}
	default:
		err = fmt.Errorf("stored version: deployments column %s is not kept", column)
	}
	return err
}

// deploymentPullRequestRegressionRefusedEvent names a successful per-SHA
// lookup that found no pull request over a held merge; the held merged_at and
// pull_request_number are kept as one unit.
const deploymentPullRequestRegressionRefusedEvent = "providersync.deployment.pull_request_regression_refused"

// applyDeploymentContract reads the held version of every row's key and
// rewrites the rows in place as the contract says. WriteEffect and the
// recovery readback's expected rows both go through it; a failed read fails
// the caller.
func applyDeploymentContract(ctx context.Context, conn storedversion.Querier, claim Claim, rows []deploymentRow, log bool) error {
	stored := make([]storedversion.Row, len(rows))
	for i, row := range rows {
		stored[i] = storedversion.Row{Values: deploymentValues(row), Carry: deploymentCarry(row)}
	}
	outcomes, err := deploymentsContract.Apply(ctx, conn, claim.OrgID, deploymentsInsert, stored)
	if err != nil {
		return err
	}
	positions, err := storedversion.Positions(deploymentsInsert)
	if err != nil {
		return err
	}
	for i := range rows {
		for _, column := range deploymentsContract.Kept() {
			if err := setDeploymentKept(&rows[i], column, stored[i].Values[positions[column]]); err != nil {
				return err
			}
		}
	}
	if log {
		logDeploymentOutcomes(ctx, claim, rows, outcomes)
	}
	return nil
}

// logDeploymentOutcomes keeps the named guard events: the lifecycle and the
// pull request pair carried over a failed lookup, and the pair refused over
// an empty lookup. Other kept columns log under the stored-version events.
func logDeploymentOutcomes(ctx context.Context, claim Claim, rows []deploymentRow, outcomes []storedversion.Outcome) {
	var lifecycle []deploymentLifecycleCarriedForward
	var pullRequests []deploymentPullRequestCarriedForward
	rest := make([]storedversion.Outcome, 0, len(outcomes))
	for i, outcome := range outcomes {
		other := storedversion.Outcome{Key: outcome.Key}
		lifecycleCarried, pullRequestCarried := false, false
		for _, column := range outcome.Carried {
			switch {
			case rows[i].LifecycleLookupFailed && (column == "status" || column == "started_at" || column == "finished_at"):
				lifecycleCarried = true
			case rows[i].PullRequestLookupFailed && (column == "merged_at" || column == "pull_request_number"):
				pullRequestCarried = true
			default:
				other.Carried = append(other.Carried, column)
			}
		}
		if lifecycleCarried {
			lifecycle = append(lifecycle, deploymentLifecycleCarriedForward{RepoID: rows[i].RepoID, DeploymentID: rows[i].DeploymentID})
		}
		if pullRequestCarried {
			pullRequests = append(pullRequests, deploymentPullRequestCarriedForward{RepoID: rows[i].RepoID, DeploymentID: rows[i].DeploymentID})
		}
		pairRefused := false
		for _, column := range outcome.Refused {
			if column == "merged_at" || column == "pull_request_number" {
				pairRefused = true
				continue
			}
			other.Refused = append(other.Refused, column)
		}
		if pairRefused && rows[i].MergedAt != nil {
			attrs := []slog.Attr{
				slog.String("org_id", claim.OrgID), slog.String("provider", claim.Provider),
				slog.String("dataset", claim.Dataset), slog.String("unit_id", claim.ID),
				slog.String("repo_id", rows[i].RepoID), logging.ProviderIDAttr("deployment_id", rows[i].DeploymentID),
				slog.Time("stored_merged_at", rows[i].MergedAt.UTC()),
			}
			if rows[i].PullRequestNumber != nil {
				attrs = append(attrs, slog.Int("stored_pull_request_number", *rows[i].PullRequestNumber))
			}
			slog.Default().LogAttrs(ctx, slog.LevelWarn, deploymentPullRequestRegressionRefusedEvent, attrs...)
		}
		rest = append(rest, other)
	}
	logDeploymentLifecycleRegressionGuarded(ctx, claim, lifecycle)
	logDeploymentPullRequestRegressionGuarded(ctx, claim, pullRequests)
	deploymentsContract.Log(ctx, claim.OrgID, storedVersionCarriedEvent, storedVersionRefusedEvent, rest)
}
