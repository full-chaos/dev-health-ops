from __future__ import annotations

import asyncio
import importlib
import logging
import os
import smtplib
from collections.abc import Mapping
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

logger = logging.getLogger(__name__)

_TEMPLATES_DIR = Path(__file__).resolve().parents[2] / "templates" / "email"


class EmailProvider:
    async def send_email(
        self,
        *,
        from_address: str,
        to_address: str,
        subject: str,
        html_content: str,
        text_content: str | None = None,
    ) -> None:
        raise NotImplementedError


class ConsoleEmailProvider(EmailProvider):
    async def send_email(
        self,
        *,
        from_address: str,
        to_address: str,
        subject: str,
        html_content: str,
        text_content: str | None = None,
    ) -> None:
        logger.info(
            "Console email provider: from=%s to=%s subject=%s html=%s text=%s",
            from_address,
            to_address,
            subject,
            html_content,
            text_content,
        )


class SmtpEmailProvider(EmailProvider):
    """SMTP provider — works with Mailpit, real SMTP relays, or any SMTP server."""

    def __init__(
        self,
        *,
        host: str = "localhost",
        port: int = 1025,
        username: str | None = None,
        password: str | None = None,
        use_tls: bool = False,
    ) -> None:
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.use_tls = use_tls

    async def send_email(
        self,
        *,
        from_address: str,
        to_address: str,
        subject: str,
        html_content: str,
        text_content: str | None = None,
    ) -> None:
        await asyncio.to_thread(
            self._send,
            from_address=from_address,
            to_address=to_address,
            subject=subject,
            html_content=html_content,
            text_content=text_content,
        )

    def _send(
        self,
        *,
        from_address: str,
        to_address: str,
        subject: str,
        html_content: str,
        text_content: str | None,
    ) -> None:
        msg = MIMEMultipart("alternative")
        msg["From"] = from_address
        msg["To"] = to_address
        msg["Subject"] = subject

        if text_content:
            msg.attach(MIMEText(text_content, "plain"))
        msg.attach(MIMEText(html_content, "html"))

        with smtplib.SMTP(self.host, self.port) as server:
            if self.use_tls:
                server.starttls()
            if self.username and self.password:
                server.login(self.username, self.password)
            server.sendmail(from_address, [to_address], msg.as_string())


class ResendEmailProvider(EmailProvider):
    def __init__(self, api_key: str) -> None:
        self.api_key = api_key

    async def send_email(
        self,
        *,
        from_address: str,
        to_address: str,
        subject: str,
        html_content: str,
        text_content: str | None = None,
    ) -> None:
        await asyncio.to_thread(
            self._send,
            from_address=from_address,
            to_address=to_address,
            subject=subject,
            html_content=html_content,
            text_content=text_content,
        )

    def _send(
        self,
        *,
        from_address: str,
        to_address: str,
        subject: str,
        html_content: str,
        text_content: str | None,
    ) -> None:
        resend = importlib.import_module("resend")
        resend.api_key = self.api_key
        payload: dict[str, object] = {
            "from": from_address,
            "to": [to_address],
            "subject": subject,
            "html": html_content,
        }
        if text_content is not None:
            payload["text"] = text_content
        resend.Emails.send(payload)


class EmailService:
    def __init__(self, provider: EmailProvider, from_address: str) -> None:
        self.provider = provider
        self.from_address = from_address

    def render_template(
        self,
        template_name: str,
        context: Mapping[str, object] | None = None,
    ) -> str:
        context_dict = dict(context or {})
        template_path = _TEMPLATES_DIR / f"{template_name}.html"
        if not template_path.exists():
            raise ValueError(f"Email template '{template_name}' not found")
        template = template_path.read_text(encoding="utf-8")
        return template.format(**context_dict)

    async def send_email(
        self,
        *,
        to_address: str,
        subject: str,
        html_content: str,
        text_content: str | None = None,
    ) -> None:
        await self.provider.send_email(
            from_address=self.from_address,
            to_address=to_address,
            subject=subject,
            html_content=html_content,
            text_content=text_content,
        )

    async def send_template_email(
        self,
        *,
        to_address: str,
        subject: str,
        template_name: str,
        context: Mapping[str, object] | None = None,
        text_content: str | None = None,
    ) -> None:
        html_content = self.render_template(template_name, context=context)
        await self.send_email(
            to_address=to_address,
            subject=subject,
            html_content=html_content,
            text_content=text_content,
        )


def get_email_service() -> EmailService:
    provider_name = os.getenv("EMAIL_PROVIDER", "console").strip().lower()
    from_address = os.getenv("EMAIL_FROM_ADDRESS", "dev-health@example.com").strip()

    if provider_name == "resend":
        api_key = os.getenv("EMAIL_API_KEY", "").strip()
        if not api_key:
            raise RuntimeError("EMAIL_API_KEY is required when EMAIL_PROVIDER=resend")
        provider: EmailProvider = ResendEmailProvider(api_key=api_key)
    elif provider_name == "smtp":
        provider = SmtpEmailProvider(
            host=os.getenv("SMTP_HOST", "localhost").strip(),
            port=int(os.getenv("SMTP_PORT", "1025")),
            username=os.getenv("SMTP_USERNAME", "").strip() or None,
            password=os.getenv("SMTP_PASSWORD", "").strip() or None,
            use_tls=os.getenv("SMTP_USE_TLS", "false").strip().lower()
            in ("true", "1", "yes"),
        )
    elif provider_name == "console":
        provider = ConsoleEmailProvider()
    else:
        raise RuntimeError(f"Unsupported email provider '{provider_name}'")

    return EmailService(provider=provider, from_address=from_address)
