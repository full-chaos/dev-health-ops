"""XXE-resistance tests for SAML XML parsing (CHAOS security sprint)."""

from __future__ import annotations

import pytest

from dev_health_ops.api.services.sso import SAMLProcessingError, SSOService

XXE_PAYLOAD = b"""<?xml version="1.0"?>
<!DOCTYPE foo [
  <!ELEMENT foo ANY >
  <!ENTITY xxe SYSTEM "file:///etc/passwd" >
]>
<foo>&xxe;</foo>
"""


def test_parse_saml_xml_rejects_doctype_payload():
    """defusedxml must reject any DOCTYPE declaration (XXE protection)."""
    with pytest.raises(SAMLProcessingError):
        SSOService._parse_saml_xml(XXE_PAYLOAD)


def test_parse_saml_xml_rejects_external_entity():
    """External entity resolution must be impossible."""
    payload = b"""<?xml version="1.0"?>
<!DOCTYPE lolz [<!ENTITY lol "lol">]>
<lolz>&lol;</lolz>
"""
    with pytest.raises(SAMLProcessingError):
        SSOService._parse_saml_xml(payload)


def test_parse_saml_xml_accepts_benign_xml():
    """Sanity: a well-formed SAML-shaped document still parses."""
    xml = b"<samlp:Response xmlns:samlp='urn:oasis:names:tc:SAML:2.0:protocol'/>"
    tree = SSOService._parse_saml_xml(xml)
    assert tree.tag.endswith("Response")
