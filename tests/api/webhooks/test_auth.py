import hashlib
import hmac

from dev_health_ops.api.webhooks.auth import (
    verify_github_signature,
    verify_gitlab_token,
    verify_jira_signature,
)


class TestGitHubSignatureValidation:
    def test_valid_signature_accepted(self):
        secret = "test-webhook-secret"
        body = b'{"action": "opened", "pull_request": {}}'

        expected = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
        signature = f"sha256={expected}"

        assert verify_github_signature(body, signature, secret) is True

    def test_invalid_signature_rejected(self):
        secret = "test-webhook-secret"
        body = b'{"action": "opened"}'
        wrong_signature = "sha256=invalid_hex_digest"

        assert verify_github_signature(body, wrong_signature, secret) is False

    def test_missing_signature_rejected(self):
        secret = "test-secret"
        body = b'{"data": "value"}'

        assert verify_github_signature(body, None, secret) is False

    def test_wrong_prefix_rejected(self):
        secret = "test-secret"
        body = b'{"data": "value"}'
        signature = "sha1=some_digest"

        assert verify_github_signature(body, signature, secret) is False

    def test_tampered_body_rejected(self):
        secret = "test-secret"
        original_body = b'{"amount": 100}'
        tampered_body = b'{"amount": 999}'

        expected = hmac.new(secret.encode(), original_body, hashlib.sha256).hexdigest()
        signature = f"sha256={expected}"

        assert verify_github_signature(tampered_body, signature, secret) is False


class TestGitLabTokenValidation:
    def test_valid_token_accepted(self):
        secret = "my-gitlab-token"
        assert verify_gitlab_token(secret, secret) is True

    def test_invalid_token_rejected(self):
        secret = "correct-token"
        wrong = "wrong-token"
        assert verify_gitlab_token(wrong, secret) is False

    def test_missing_token_rejected(self):
        secret = "my-token"
        assert verify_gitlab_token(None, secret) is False

    def test_empty_token_rejected(self):
        secret = "my-token"
        assert verify_gitlab_token("", secret) is False


class TestJiraSignatureValidation:
    def test_no_secret_allows_all(self):
        body = b'{"issue": {"key": "PROJ-123"}}'
        assert verify_jira_signature(body, None, "") is True

    def test_valid_hmac_accepted(self):
        secret = "jira-secret"
        body = b'{"webhookEvent": "jira:issue_created"}'

        expected = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()

        assert verify_jira_signature(body, expected, secret) is True

    def test_invalid_hmac_rejected(self):
        secret = "jira-secret"
        body = b'{"webhookEvent": "jira:issue_created"}'

        assert verify_jira_signature(body, "invalid", secret) is False
