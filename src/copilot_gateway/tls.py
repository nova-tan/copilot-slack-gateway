"""Optional TLS compatibility for enterprise interception proxies.

Python 3.13+ enables VERIFY_X509_STRICT by default, which can reject an
intermediate CA even though the chain is otherwise valid and trusted. Clearing
that one flag keeps normal certificate verification while tolerating legacy CA
encodings. Enable this workaround explicitly with
COPILOT_RELAX_X509_STRICT=true.
"""

from __future__ import annotations

import ssl


def relax_strict_x509() -> None:
    flag = getattr(ssl, "VERIFY_X509_STRICT", None)
    if flag is None:
        return
    original = ssl.create_default_context

    def create_default_context(*args, **kwargs):  # noqa: D103
        ctx = original(*args, **kwargs)
        ctx.verify_flags &= ~flag
        return ctx

    ssl.create_default_context = create_default_context
