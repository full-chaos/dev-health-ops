"""Tests for email providers and get_email_service factory."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from dev_health_ops.api.services.email import (
    ConsoleEmailProvider,
    SmtpEmailProvider,
    get_email_service,
)

# ---------------------------------------------------------------------------
# SmtpEmailProvider
# ---------------------------------------------------------------------------


class TestSmtpEmailProvider:
    def test_defaults(self):
        provider = SmtpEmailProvider()
        assert provider.host == "localhost"
        assert provider.port == 1025
        assert provider.username is None
        assert provider.password is None
        assert provider.use_tls is False

    def test_custom_config(self):
        provider = SmtpEmailProvider(
            host="smtp.example.com",
            port=587,
            username="user",
            password="pass",
            use_tls=True,
        )
        assert provider.host == "smtp.example.com"
        assert provider.port == 587
        assert provider.use_tls is True

    @pytest.mark.asyncio
    async def test_send_email_constructs_mime_and_sends(self):
        provider = SmtpEmailProvider(host="mail", port=2525)

        mock_smtp_instance = MagicMock()
        mock_smtp_cls = MagicMock(return_value=mock_smtp_instance)
        mock_smtp_instance.__enter__ = MagicMock(return_value=mock_smtp_instance)
        mock_smtp_instance.__exit__ = MagicMock(return_value=False)

        with patch("dev_health_ops.api.services.email.smtplib.SMTP", mock_smtp_cls):
            await provider.send_email(
                from_address="from@test.com",
                to_address="to@test.com",
                subject="Test Subject",
                html_content="<p>Hello</p>",
                text_content="Hello",
            )

        mock_smtp_cls.assert_called_once_with("mail", 2525)
        mock_smtp_instance.sendmail.assert_called_once()
        args = mock_smtp_instance.sendmail.call_args
        assert args[0][0] == "from@test.com"
        assert args[0][1] == ["to@test.com"]
        # Verify the MIME message contains our content
        raw_msg = args[0][2]
        assert "Test Subject" in raw_msg
        assert "<p>Hello</p>" in raw_msg

    @pytest.mark.asyncio
    async def test_send_email_with_tls_and_auth(self):
        provider = SmtpEmailProvider(
            host="smtp.example.com",
            port=587,
            username="user",
            password="secret",
            use_tls=True,
        )

        mock_smtp_instance = MagicMock()
        mock_smtp_cls = MagicMock(return_value=mock_smtp_instance)
        mock_smtp_instance.__enter__ = MagicMock(return_value=mock_smtp_instance)
        mock_smtp_instance.__exit__ = MagicMock(return_value=False)

        with patch("dev_health_ops.api.services.email.smtplib.SMTP", mock_smtp_cls):
            await provider.send_email(
                from_address="from@test.com",
                to_address="to@test.com",
                subject="Test",
                html_content="<p>Hi</p>",
            )

        mock_smtp_instance.starttls.assert_called_once()
        mock_smtp_instance.login.assert_called_once_with("user", "secret")


# ---------------------------------------------------------------------------
# get_email_service factory
# ---------------------------------------------------------------------------


class TestGetEmailService:
    def test_smtp_provider_from_env(self, monkeypatch):
        monkeypatch.setenv("EMAIL_PROVIDER", "smtp")
        monkeypatch.setenv("SMTP_HOST", "mailpit")
        monkeypatch.setenv("SMTP_PORT", "1025")

        svc = get_email_service()
        assert isinstance(svc.provider, SmtpEmailProvider)
        assert svc.provider.host == "mailpit"
        assert svc.provider.port == 1025

    def test_console_provider_from_env(self, monkeypatch):
        monkeypatch.setenv("EMAIL_PROVIDER", "console")
        svc = get_email_service()
        assert isinstance(svc.provider, ConsoleEmailProvider)

    def test_default_is_console(self, monkeypatch):
        monkeypatch.delenv("EMAIL_PROVIDER", raising=False)
        svc = get_email_service()
        assert isinstance(svc.provider, ConsoleEmailProvider)

    def test_unsupported_provider_raises(self, monkeypatch):
        monkeypatch.setenv("EMAIL_PROVIDER", "sendgrid")
        with pytest.raises(RuntimeError, match="Unsupported email provider"):
            get_email_service()

    def test_smtp_optional_auth(self, monkeypatch):
        monkeypatch.setenv("EMAIL_PROVIDER", "smtp")
        monkeypatch.setenv("SMTP_USERNAME", "user")
        monkeypatch.setenv("SMTP_PASSWORD", "pass")
        monkeypatch.setenv("SMTP_USE_TLS", "true")

        svc = get_email_service()
        provider = svc.provider
        assert isinstance(provider, SmtpEmailProvider)
        assert provider.username == "user"
        assert provider.password == "pass"
        assert provider.use_tls is True

    def test_smtp_empty_auth_becomes_none(self, monkeypatch):
        monkeypatch.setenv("EMAIL_PROVIDER", "smtp")
        monkeypatch.setenv("SMTP_USERNAME", "")
        monkeypatch.setenv("SMTP_PASSWORD", "")

        svc = get_email_service()
        provider = svc.provider
        assert isinstance(provider, SmtpEmailProvider)
        assert provider.username is None
        assert provider.password is None
