from __future__ import annotations

import base64
import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dev_health_ops.api.services.sso import SSOService
from dev_health_ops.models.sso import SSOProvider


def _saml_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


class TestSSOServiceSAML:
    @pytest.mark.asyncio
    async def test_process_saml_response_success(self):
        org_id = uuid.uuid4()
        provider = SSOProvider(
            org_id=org_id,
            name="Example SAML",
            protocol="saml",
            config={
                "entity_id": "https://idp.example.com/metadata",
                "sso_url": "https://idp.example.com/sso",
                "certificate": "-----BEGIN CERTIFICATE-----\nMIIF...\n-----END CERTIFICATE-----",
                "sp_entity_id": "https://app.example.com/saml/metadata",
                "sp_acs_url": "https://app.example.com/saml/acs",
                "attribute_mapping": {"email": "email", "full_name": "name"},
            },
        )

        now = datetime.now(timezone.utc)
        assertion_not_before = _saml_timestamp(now - timedelta(minutes=1))
        assertion_not_after = _saml_timestamp(now + timedelta(minutes=5))
        subject_not_after = _saml_timestamp(now + timedelta(minutes=5))

        xml = f"""
<samlp:Response xmlns:samlp="urn:oasis:names:tc:SAML:2.0:protocol"
    xmlns:saml="urn:oasis:names:tc:SAML:2.0:assertion">
    <saml:Issuer>https://idp.example.com/metadata</saml:Issuer>
    <samlp:Status>
        <samlp:StatusCode Value="urn:oasis:names:tc:SAML:2.0:status:Success"/>
    </samlp:Status>
    <saml:Assertion>
        <saml:Issuer>https://idp.example.com/metadata</saml:Issuer>
        <saml:Subject>
            <saml:NameID>user@example.com</saml:NameID>
            <saml:SubjectConfirmation>
                <saml:SubjectConfirmationData
                    Recipient="https://app.example.com/saml/acs"
                    NotOnOrAfter="{subject_not_after}" />
            </saml:SubjectConfirmation>
        </saml:Subject>
        <saml:Conditions NotBefore="{assertion_not_before}" NotOnOrAfter="{assertion_not_after}">
            <saml:AudienceRestriction>
                <saml:Audience>https://app.example.com/saml/metadata</saml:Audience>
            </saml:AudienceRestriction>
        </saml:Conditions>
        <saml:AttributeStatement>
            <saml:Attribute Name="email">
                <saml:AttributeValue>user@example.com</saml:AttributeValue>
            </saml:Attribute>
            <saml:Attribute Name="name">
                <saml:AttributeValue>Example User</saml:AttributeValue>
            </saml:Attribute>
        </saml:AttributeStatement>
    </saml:Assertion>
</samlp:Response>
""".strip()

        saml_response = base64.b64encode(xml.encode("utf-8")).decode("utf-8")

        service = SSOService(MagicMock())
        with patch.object(
            SSOService, "_validate_saml_signature", return_value=None
        ) as validator:
            result = await service.process_saml_response(
                provider=provider,
                saml_response=saml_response,
                relay_state=None,
            )

        validator.assert_called_once()
        assert result["email"] == "user@example.com"
        assert result["full_name"] == "Example User"


class TestSSOServiceOIDC:
    @pytest.mark.asyncio
    async def test_process_oidc_callback_success(self):
        org_id = uuid.uuid4()
        provider = SSOProvider(
            org_id=org_id,
            name="Example OIDC",
            protocol="oidc",
            config={
                "client_id": "client-123",
                "issuer": "https://issuer.example.com",
                "token_endpoint": "https://issuer.example.com/token",
                "userinfo_endpoint": "https://issuer.example.com/userinfo",
                "jwks_uri": "https://issuer.example.com/jwks",
                "claim_mapping": {"full_name": "name"},
            },
            encrypted_secrets={"expected_state": "state-123"},
        )

        service = SSOService(MagicMock(), base_url="https://app.example.com")
        with (
            patch.object(
                SSOService,
                "_exchange_oidc_code",
                new=AsyncMock(
                    return_value={"access_token": "access", "id_token": "id-token"}
                ),
            ),
            patch.object(
                SSOService,
                "_validate_id_token",
                new=AsyncMock(
                    return_value={
                        "sub": "user-123",
                        "email": "user@example.com",
                        "name": "OIDC User",
                    }
                ),
            ),
            patch.object(SSOService, "_fetch_userinfo", new=AsyncMock(return_value={})),
        ):
            result = await service.process_oidc_callback(
                provider=provider,
                code="auth-code",
                state="state-123",
                code_verifier=None,
            )

        assert result["email"] == "user@example.com"
        assert result["full_name"] == "OIDC User"
        assert result["external_id"] == "user-123"

    @pytest.mark.asyncio
    async def test_process_oidc_callback_state_mismatch(self):
        """Test that OIDC state mismatch is properly detected."""
        from dev_health_ops.api.services.sso import OIDCProcessingError

        org_id = uuid.uuid4()
        provider = SSOProvider(
            org_id=org_id,
            name="Example OIDC",
            protocol="oidc",
            config={
                "client_id": "client-123",
                "issuer": "https://issuer.example.com",
            },
            encrypted_secrets={"expected_state": "expected-state"},
        )

        service = SSOService(MagicMock(), base_url="https://app.example.com")
        with pytest.raises(OIDCProcessingError, match="state mismatch"):
            await service.process_oidc_callback(
                provider=provider,
                code="auth-code",
                state="wrong-state",
                code_verifier=None,
            )

    @pytest.mark.asyncio
    async def test_process_oidc_callback_missing_state(self):
        """Test that missing OIDC state is properly detected."""
        from dev_health_ops.api.services.sso import OIDCProcessingError

        org_id = uuid.uuid4()
        provider = SSOProvider(
            org_id=org_id,
            name="Example OIDC",
            protocol="oidc",
            config={
                "client_id": "client-123",
                "issuer": "https://issuer.example.com",
            },
        )

        service = SSOService(MagicMock(), base_url="https://app.example.com")
        with pytest.raises(OIDCProcessingError, match="Missing OIDC state"):
            await service.process_oidc_callback(
                provider=provider,
                code="auth-code",
                state=None,
                code_verifier=None,
            )

    @pytest.mark.asyncio
    async def test_process_oidc_callback_missing_email(self):
        """Test that missing email in OIDC claims is properly detected."""
        from dev_health_ops.api.services.sso import OIDCProcessingError

        org_id = uuid.uuid4()
        provider = SSOProvider(
            org_id=org_id,
            name="Example OIDC",
            protocol="oidc",
            config={
                "client_id": "client-123",
                "issuer": "https://issuer.example.com",
                "token_endpoint": "https://issuer.example.com/token",
                "jwks_uri": "https://issuer.example.com/jwks",
            },
            encrypted_secrets={"expected_state": "state-123"},
        )

        service = SSOService(MagicMock(), base_url="https://app.example.com")
        with (
            patch.object(
                SSOService,
                "_exchange_oidc_code",
                new=AsyncMock(
                    return_value={"access_token": "access", "id_token": "id-token"}
                ),
            ),
            patch.object(
                SSOService,
                "_validate_id_token",
                new=AsyncMock(
                    return_value={
                        "sub": "user-123",
                        "name": "OIDC User",
                        # Missing email
                    }
                ),
            ),
            patch.object(SSOService, "_fetch_userinfo", new=AsyncMock(return_value={})),
        ):
            with pytest.raises(OIDCProcessingError, match="missing email"):
                await service.process_oidc_callback(
                    provider=provider,
                    code="auth-code",
                    state="state-123",
                    code_verifier=None,
                )


class TestSSOServiceSAMLErrors:
    @pytest.mark.asyncio
    async def test_process_saml_response_expired(self):
        """Test that expired SAML assertions are rejected."""
        from dev_health_ops.api.services.sso import SAMLProcessingError

        org_id = uuid.uuid4()
        provider = SSOProvider(
            org_id=org_id,
            name="Example SAML",
            protocol="saml",
            config={
                "entity_id": "https://idp.example.com/metadata",
                "sso_url": "https://idp.example.com/sso",
                "certificate": "-----BEGIN CERTIFICATE-----\nMIIF...\n-----END CERTIFICATE-----",
                "sp_entity_id": "https://app.example.com/saml/metadata",
                "sp_acs_url": "https://app.example.com/saml/acs",
                "attribute_mapping": {"email": "email", "full_name": "name"},
            },
        )

        now = datetime.now(timezone.utc)
        # Set assertion to be expired 10 minutes ago
        assertion_not_before = _saml_timestamp(now - timedelta(minutes=20))
        assertion_not_after = _saml_timestamp(now - timedelta(minutes=10))
        subject_not_after = _saml_timestamp(now - timedelta(minutes=10))

        xml = f"""
<samlp:Response xmlns:samlp="urn:oasis:names:tc:SAML:2.0:protocol"
    xmlns:saml="urn:oasis:names:tc:SAML:2.0:assertion">
    <saml:Issuer>https://idp.example.com/metadata</saml:Issuer>
    <samlp:Status>
        <samlp:StatusCode Value="urn:oasis:names:tc:SAML:2.0:status:Success"/>
    </samlp:Status>
    <saml:Assertion>
        <saml:Issuer>https://idp.example.com/metadata</saml:Issuer>
        <saml:Subject>
            <saml:NameID>user@example.com</saml:NameID>
            <saml:SubjectConfirmation>
                <saml:SubjectConfirmationData
                    Recipient="https://app.example.com/saml/acs"
                    NotOnOrAfter="{subject_not_after}" />
            </saml:SubjectConfirmation>
        </saml:Subject>
        <saml:Conditions NotBefore="{assertion_not_before}" NotOnOrAfter="{assertion_not_after}">
            <saml:AudienceRestriction>
                <saml:Audience>https://app.example.com/saml/metadata</saml:Audience>
            </saml:AudienceRestriction>
        </saml:Conditions>
        <saml:AttributeStatement>
            <saml:Attribute Name="email">
                <saml:AttributeValue>user@example.com</saml:AttributeValue>
            </saml:Attribute>
        </saml:AttributeStatement>
    </saml:Assertion>
</samlp:Response>
""".strip()

        saml_response = base64.b64encode(xml.encode("utf-8")).decode("utf-8")

        service = SSOService(MagicMock())
        with patch.object(SSOService, "_validate_saml_signature", return_value=None):
            with pytest.raises(SAMLProcessingError, match="expired"):
                await service.process_saml_response(
                    provider=provider,
                    saml_response=saml_response,
                    relay_state=None,
                )

    @pytest.mark.asyncio
    async def test_process_saml_response_missing_email(self):
        """Test that SAML responses without email are rejected."""
        from dev_health_ops.api.services.sso import SAMLProcessingError

        org_id = uuid.uuid4()
        provider = SSOProvider(
            org_id=org_id,
            name="Example SAML",
            protocol="saml",
            config={
                "entity_id": "https://idp.example.com/metadata",
                "sso_url": "https://idp.example.com/sso",
                "certificate": "-----BEGIN CERTIFICATE-----\nMIIF...\n-----END CERTIFICATE-----",
                "sp_entity_id": "https://app.example.com/saml/metadata",
                "sp_acs_url": "https://app.example.com/saml/acs",
                "attribute_mapping": {"email": "email", "full_name": "name"},
            },
        )

        now = datetime.now(timezone.utc)
        assertion_not_before = _saml_timestamp(now - timedelta(minutes=1))
        assertion_not_after = _saml_timestamp(now + timedelta(minutes=5))
        subject_not_after = _saml_timestamp(now + timedelta(minutes=5))

        xml = f"""
<samlp:Response xmlns:samlp="urn:oasis:names:tc:SAML:2.0:protocol"
    xmlns:saml="urn:oasis:names:tc:SAML:2.0:assertion">
    <saml:Issuer>https://idp.example.com/metadata</saml:Issuer>
    <samlp:Status>
        <samlp:StatusCode Value="urn:oasis:names:tc:SAML:2.0:status:Success"/>
    </samlp:Status>
    <saml:Assertion>
        <saml:Issuer>https://idp.example.com/metadata</saml:Issuer>
        <saml:Subject>
            <saml:NameID>user@example.com</saml:NameID>
            <saml:SubjectConfirmation>
                <saml:SubjectConfirmationData
                    Recipient="https://app.example.com/saml/acs"
                    NotOnOrAfter="{subject_not_after}" />
            </saml:SubjectConfirmation>
        </saml:Subject>
        <saml:Conditions NotBefore="{assertion_not_before}" NotOnOrAfter="{assertion_not_after}">
            <saml:AudienceRestriction>
                <saml:Audience>https://app.example.com/saml/metadata</saml:Audience>
            </saml:AudienceRestriction>
        </saml:Conditions>
        <saml:AttributeStatement>
            <saml:Attribute Name="name">
                <saml:AttributeValue>Example User</saml:AttributeValue>
            </saml:Attribute>
        </saml:AttributeStatement>
    </saml:Assertion>
</samlp:Response>
""".strip()

        saml_response = base64.b64encode(xml.encode("utf-8")).decode("utf-8")

        service = SSOService(MagicMock())
        with patch.object(SSOService, "_validate_saml_signature", return_value=None):
            with pytest.raises(SAMLProcessingError, match="missing email"):
                await service.process_saml_response(
                    provider=provider,
                    saml_response=saml_response,
                    relay_state=None,
                )
