"""SSO enterprise auth sub-package.

SAML, OIDC, and OAuth endpoints, gated behind enterprise license.
"""

from .router import sso_router

__all__ = ["sso_router"]
