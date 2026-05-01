"""Database connection helpers for tides_changes.

The database URL is read from the environment variable ``TIDES_DB_URL``, which
should be a PostgreSQL connection string in SQLAlchemy format, e.g.::

    postgresql+psycopg2://user:password@host:5432/tides

A module-level engine and session factory are created on first use via
:func:`get_engine` and :func:`get_session`.
"""

import os

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

_engine = None
_Session = None


def get_engine():
    """Return (and lazily create) the shared SQLAlchemy engine."""
    global _engine
    if _engine is None:
        db_url = os.environ.get("TIDES_DB_URL")
        if not db_url:
            raise RuntimeError(
                "Environment variable TIDES_DB_URL is not set. "
                "Set it to a SQLAlchemy-compatible PostgreSQL connection "
                "string, e.g. "
                "'postgresql+psycopg2://user:pass@host:5432/tides'."
            )
        _engine = create_engine(db_url, pool_pre_ping=True)
    return _engine


def get_session() -> Session:
    """Return a new SQLAlchemy session bound to the shared engine."""
    global _Session
    if _Session is None:
        _Session = sessionmaker(bind=get_engine())
    return _Session()
