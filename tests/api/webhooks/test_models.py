from dev_health_ops.api.webhooks.models import (
    WebhookEvent,
    WebhookEventType,
    WebhookProvider,
    map_github_event,
    map_gitlab_event,
    map_jira_event,
)


class TestGitHubEventMapping:
    def test_push_event(self):
        assert map_github_event("push") == WebhookEventType.PUSH

    def test_pull_request_event(self):
        assert map_github_event("pull_request") == WebhookEventType.PULL_REQUEST

    def test_issues_opened(self):
        assert map_github_event("issues", "opened") == WebhookEventType.ISSUE_CREATED

    def test_issues_closed(self):
        assert map_github_event("issues", "closed") == WebhookEventType.ISSUE_CLOSED

    def test_issues_deleted(self):
        assert map_github_event("issues", "deleted") == WebhookEventType.ISSUE_DELETED

    def test_issues_other_action(self):
        assert map_github_event("issues", "labeled") == WebhookEventType.ISSUE_UPDATED

    def test_deployment(self):
        assert map_github_event("deployment") == WebhookEventType.DEPLOYMENT

    def test_check_run(self):
        assert map_github_event("check_run") == WebhookEventType.CHECK_RUN

    def test_unknown_event(self):
        assert map_github_event("unknown_event") == WebhookEventType.UNKNOWN


class TestGitLabEventMapping:
    def test_push_hook(self):
        assert map_gitlab_event("Push Hook") == WebhookEventType.PUSH

    def test_tag_push_hook(self):
        assert map_gitlab_event("Tag Push Hook") == WebhookEventType.PUSH

    def test_merge_request_hook(self):
        assert map_gitlab_event("Merge Request Hook") == WebhookEventType.MERGE_REQUEST

    def test_issue_hook_open(self):
        assert map_gitlab_event("Issue Hook", "open") == WebhookEventType.ISSUE_CREATED

    def test_issue_hook_close(self):
        assert map_gitlab_event("Issue Hook", "close") == WebhookEventType.ISSUE_CLOSED

    def test_issue_hook_other(self):
        assert (
            map_gitlab_event("Issue Hook", "update") == WebhookEventType.ISSUE_UPDATED
        )

    def test_pipeline_hook(self):
        assert map_gitlab_event("Pipeline Hook") == WebhookEventType.PIPELINE

    def test_unknown_event(self):
        assert map_gitlab_event("Unknown Hook") == WebhookEventType.UNKNOWN


class TestJiraEventMapping:
    def test_issue_created(self):
        assert map_jira_event("jira:issue_created") == WebhookEventType.ISSUE_CREATED

    def test_issue_updated(self):
        assert map_jira_event("jira:issue_updated") == WebhookEventType.ISSUE_UPDATED

    def test_issue_deleted(self):
        assert map_jira_event("jira:issue_deleted") == WebhookEventType.ISSUE_DELETED

    def test_unknown_event(self):
        assert map_jira_event("jira:unknown") == WebhookEventType.UNKNOWN


class TestWebhookEventModel:
    def test_event_has_auto_generated_id(self):
        event = WebhookEvent(
            provider=WebhookProvider.GITHUB,
            event_type=WebhookEventType.PUSH,
            raw_event_type="push",
        )
        assert event.id is not None

    def test_event_has_received_timestamp(self):
        event = WebhookEvent(
            provider=WebhookProvider.GITLAB,
            event_type=WebhookEventType.MERGE_REQUEST,
            raw_event_type="Merge Request Hook",
        )
        assert event.received_at is not None

    def test_event_stores_payload(self):
        payload = {"key": "PROJ-123", "summary": "Test issue"}
        event = WebhookEvent(
            provider=WebhookProvider.JIRA,
            event_type=WebhookEventType.ISSUE_CREATED,
            raw_event_type="jira:issue_created",
            payload=payload,
        )
        assert event.payload == payload
