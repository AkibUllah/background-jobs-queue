"""FastAPI dashboard API (and static host for the built React UI)."""

from jobq.dashboard.api import create_app

__all__ = ["create_app"]
