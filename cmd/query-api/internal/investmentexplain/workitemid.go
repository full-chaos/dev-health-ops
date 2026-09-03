package investmentexplain

import "fmt"

// deriveWorkItemID ports external_ingest/ids.py's derive_work_item_id
// (ids.py:54-86) exactly. instance is the repo full name (owner/repo or
// group/project) for system in {"github", "gitlab"} -- ignored for
// jira/linear, where external_key alone is already globally unique within
// the org's namespace. workItemType disambiguates issue vs pr/merge_request
// for github/gitlab; any other value (including "") is a plain issue.
func deriveWorkItemID(system, instance, externalKey, workItemType string) string {
	switch system {
	case "jira":
		return "jira:" + externalKey
	case "linear":
		return "linear:" + externalKey
	case "github":
		if workItemType == "pr" {
			return fmt.Sprintf("ghpr:%s#%s", instance, externalKey)
		}
		return fmt.Sprintf("gh:%s#%s", instance, externalKey)
	case "gitlab":
		if workItemType == "merge_request" {
			return fmt.Sprintf("gitlab:%s!%s", instance, externalKey)
		}
		return fmt.Sprintf("gitlab:%s#%s", instance, externalKey)
	default:
		return fmt.Sprintf("custom:%s:%s", instance, externalKey)
	}
}
