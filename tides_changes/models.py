"""SQLAlchemy models for the tides_changes package.

This module defines:
  - TidesMaster: a read-only reflection of the existing tides.tides_master
    table (owned by tides-ingest) used here only for lookups.
  - TidesChangesShared: the new bookkeeping table that records which survey
    triggered each transient and which survey name was used when submitting
    the transient to 4MOST.
"""

from datetime import datetime, timezone

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Double,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


class TidesMaster(Base):
    """Read-only reflection of tides.tides_master (owned by tides-ingest).

    Only the columns relevant to this package are listed here.  The table is
    expected to live in a PostgreSQL schema called ``tides``.
    """

    __tablename__ = "tides_master"
    __table_args__ = {"schema": "tides"}

    id = Column(Integer, primary_key=True)
    transient_name = Column(String(255), nullable=False, unique=True)
    ra = Column(Double)
    dec = Column(Double)
    pk_4most = Column(Integer)
    ostd_u_obj_id = Column(String(255))
    created_at = Column(DateTime(timezone=True))


class TidesChangesShared(Base):
    """Bookkeeping table for transients shared between TiDES and CHANGES.

    A row is written here whenever the CHANGES Kafka consumer encounters a
    transient, regardless of whether TiDES has already seen it.  The
    ``first_trigger`` column records which survey "got there first", and
    ``survey_submitted_as`` records the survey name that was ultimately used
    when calling ``submit_transients`` against the 4MOST API.

    The ``tides_triggered`` / ``changes_triggered`` flags are updated by their
    respective flows so that the table reflects the full picture even when
    events arrive out of order.
    """

    __tablename__ = "tideschanges_shared"
    __table_args__ = (
        UniqueConstraint("transient_name", name="uq_tideschanges_shared_name"),
        {"schema": "tides"},
    )

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Transient identification
    transient_name = Column(String(255), nullable=False)
    ra = Column(Double)
    dec = Column(Double)

    # TiDES bookkeeping
    tides_triggered = Column(Boolean, default=False, nullable=False)
    tides_triggered_at = Column(DateTime(timezone=True))

    # CHANGES bookkeeping
    changes_triggered = Column(Boolean, default=False, nullable=False)
    changes_triggered_at = Column(DateTime(timezone=True))

    # Derived: which survey got there first
    # Values: 'tides' | 'changes' | None (not yet submitted by either)
    first_trigger = Column(String(50))

    # Whether both surveys have triggered this transient
    is_shared = Column(Boolean, default=False, nullable=False)

    # 4MOST submission result
    # The survey name used when submitting to 4MOST ('TiDES' or 'CHANGES')
    survey_submitted_as = Column(String(50))
    pk_4most = Column(Integer)
    ostd_u_obj_id = Column(String(255))
    submitted_at = Column(DateTime(timezone=True))

    # Row metadata
    created_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    updated_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
