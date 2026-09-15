"""Slack Socket Mode gateway driving persistent GitHub Copilot CLI ACP sessions."""

import os

from .env import load_local_env
from .tls import relax_strict_x509

load_local_env()
if os.environ.get("COPILOT_RELAX_X509_STRICT", "").strip().lower() in {"1", "true", "yes", "on"}:
    relax_strict_x509()

__version__ = "0.1.0"
