"""Prefect tasks for tideschanges_shared bookkeeping.

These tasks handle the database reads and writes needed to decide which survey
name should be used when a transient is submitted to 4MOST, and to record the
outcome in ``tides.tideschanges_shared``.

The core business logic lives in plain functions (no Prefect dependency) so
they are easy to test in isolation.  Each ``@task``-decorated function is a
thin wrapper that adds logging and Prefect observability.

Dependency on tides-ingest
--------------------------
The ``submit_transients`` helper and the ``check_tides_master`` helper are
imported from the ``tides_ingest`` package (the sister repo).  When running
locally without that package installed, ``ImportError`` will be raised at
import time with a clear message.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from prefect import get_run_logger, task
from sqlalchemy.orm import Session

from tides_changes.models import TidesChangesShared, TidesMaster

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Survey name constants – kept here so they are easy to change in one place.
# ---------------------------------------------------------------------------
SURVEY_TIDES = "TiDES"
SURVEY_CHANGES = "CHANGES"


# ---------------------------------------------------------------------------
# Pure business logic (no Prefect dependency – easy to unit-test)
# ---------------------------------------------------------------------------


def query_tides_master(transient_name: str, session: Session) -> TidesMaster | None:
    """Return the tides_master row for *transient_name*, or ``None``."""
    return (
        session.query(TidesMaster)
        .filter(TidesMaster.transient_name == transient_name)
        .first()
    )


def query_tideschanges_shared(
    transient_name: str, session: Session
) -> TidesChangesShared | None:
    """Return the tideschanges_shared row for *transient_name*, or ``None``."""
    return (
        session.query(TidesChangesShared)
        .filter(TidesChangesShared.transient_name == transient_name)
        .first()
    )


def upsert_changes_trigger(
    transient: dict[str, Any],
    is_shared: bool,
    session: Session,
) -> TidesChangesShared:
    """Write (or update) the tideschanges_shared row for a CHANGES transient.

    Parameters
    ----------
    transient:
        Dict with at least ``transient_name``, and optionally ``ra`` / ``dec``.
    is_shared:
        ``True`` when TiDES has already recorded this transient in
        ``tides_master`` (i.e. TiDES triggered first).
    session:
        An open SQLAlchemy session.

    Returns
    -------
    TidesChangesShared
        The newly created or updated row.
    """
    now = datetime.now(timezone.utc)
    name = transient["transient_name"]

    existing = query_tideschanges_shared(name, session)

    if existing is None:
        first_trigger = SURVEY_TIDES.lower() if is_shared else SURVEY_CHANGES.lower()
        row = TidesChangesShared(
            transient_name=name,
            ra=transient.get("ra"),
            dec=transient.get("dec"),
            changes_triggered=True,
            changes_triggered_at=now,
            tides_triggered=is_shared,
            tides_triggered_at=now if is_shared else None,
            first_trigger=first_trigger,
            is_shared=is_shared,
        )
        session.add(row)
        logger.info(
            "Created tideschanges_shared row for '%s': is_shared=%s, first_trigger='%s'.",
            name,
            is_shared,
            first_trigger,
        )
    else:
        existing.changes_triggered = True
        if existing.changes_triggered_at is None:
            existing.changes_triggered_at = now
        if is_shared and not existing.tides_triggered:
            existing.tides_triggered = True
            existing.tides_triggered_at = now
        existing.is_shared = existing.tides_triggered and existing.changes_triggered
        existing.updated_at = now
        row = existing
        logger.info(
            "Updated tideschanges_shared row for '%s': is_shared=%s.",
            name,
            row.is_shared,
        )

    session.commit()
    return row


def upsert_tides_trigger(
    transient_name: str,
    session: Session,
) -> TidesChangesShared | None:
    """Update the tideschanges_shared row to mark that TiDES has triggered.

    Called from the TiDES (tides-ingest) flow after it has confirmed that a
    transient also exists in tideschanges_shared (i.e. CHANGES triggered it
    first).

    Returns ``None`` when no existing row is found (the transient is
    TiDES-only and does not need a tideschanges_shared entry).
    """
    now = datetime.now(timezone.utc)

    row = query_tideschanges_shared(transient_name, session)
    if row is None:
        logger.debug("No tideschanges_shared row for '%s'; skipping.", transient_name)
        return None

    if not row.tides_triggered:
        row.tides_triggered = True
        row.tides_triggered_at = now
        row.is_shared = True
        row.updated_at = now
        session.commit()
        logger.info(
            "Marked '%s' as TiDES-triggered in tideschanges_shared.", transient_name
        )
    return row


def resolve_survey_name(shared_row: TidesChangesShared) -> str:
    """Return the survey name to use when submitting *transient* to 4MOST.

    Rules
    -----
    * If only CHANGES has triggered: submit as ``CHANGES``.
    * If only TiDES has triggered: submit as ``TiDES``.
    * If both have triggered: the *first* trigger wins (``first_trigger``
      column).  This matches the requirement that the 4MOST transient database
      can only list one survey per transient.
    """
    if not shared_row.is_shared:
        return SURVEY_TIDES if shared_row.tides_triggered else SURVEY_CHANGES

    if shared_row.first_trigger == SURVEY_TIDES.lower():
        return SURVEY_TIDES
    return SURVEY_CHANGES


def write_submission_result(
    shared_row: TidesChangesShared,
    survey_submitted_as: str,
    pk_4most: int | None,
    ostd_u_obj_id: str | None,
    session: Session,
) -> TidesChangesShared:
    """Persist the 4MOST submission result back into tideschanges_shared."""
    now = datetime.now(timezone.utc)
    shared_row.survey_submitted_as = survey_submitted_as
    shared_row.pk_4most = pk_4most
    shared_row.ostd_u_obj_id = ostd_u_obj_id
    shared_row.submitted_at = now
    shared_row.updated_at = now
    session.commit()
    logger.info(
        "Recorded 4MOST submission for '%s': survey='%s', pk_4most=%s.",
        shared_row.transient_name,
        survey_submitted_as,
        pk_4most,
    )
    return shared_row


# ---------------------------------------------------------------------------
# Prefect task wrappers
# ---------------------------------------------------------------------------


@task(name="check_tides_master")
def check_tides_master(transient_name: str, session: Session) -> TidesMaster | None:
    """Return the tides_master row for *transient_name*, or ``None``.

    This task queries ``tides.tides_master`` which is owned and written by the
    tides-ingest pipeline.  The result tells us whether TiDES has already
    submitted this transient to 4MOST.
    """
    log = get_run_logger()
    row = query_tides_master(transient_name, session)
    if row:
        log.info("Transient '%s' found in tides_master (shared target).", transient_name)
    else:
        log.info(
            "Transient '%s' not found in tides_master (CHANGES-only target).",
            transient_name,
        )
    return row


@task(name="check_tideschanges_shared")
def check_tideschanges_shared(
    transient_name: str, session: Session
) -> TidesChangesShared | None:
    """Return the tideschanges_shared row for *transient_name*, or ``None``."""
    return query_tideschanges_shared(transient_name, session)


@task(name="record_changes_trigger")
def record_changes_trigger(
    transient: dict[str, Any],
    is_shared: bool,
    session: Session,
) -> TidesChangesShared:
    """Write (or update) the tideschanges_shared row for a CHANGES transient."""
    log = get_run_logger()
    row = upsert_changes_trigger(transient, is_shared, session)
    log.info(
        "Recorded CHANGES trigger for '%s': is_shared=%s.",
        row.transient_name,
        row.is_shared,
    )
    return row


@task(name="record_tides_trigger")
def record_tides_trigger(
    transient_name: str,
    session: Session,
) -> TidesChangesShared | None:
    """Update the tideschanges_shared row to mark that TiDES has triggered.

    Called from the TiDES (tides-ingest) flow after it has confirmed that a
    transient also exists in tideschanges_shared (i.e. CHANGES triggered it
    first).
    """
    log = get_run_logger()
    row = upsert_tides_trigger(transient_name, session)
    if row:
        log.info(
            "Marked '%s' as TiDES-triggered in tideschanges_shared.", transient_name
        )
    return row


@task(name="determine_survey_name")
def determine_survey_name(shared_row: TidesChangesShared) -> str:
    """Return the survey name to use when submitting *transient* to 4MOST."""
    return resolve_survey_name(shared_row)


@task(name="record_submission_result")
def record_submission_result(
    shared_row: TidesChangesShared,
    survey_submitted_as: str,
    pk_4most: int | None,
    ostd_u_obj_id: str | None,
    session: Session,
) -> TidesChangesShared:
    """Persist the 4MOST submission result back into tideschanges_shared."""
    log = get_run_logger()
    row = write_submission_result(
        shared_row, survey_submitted_as, pk_4most, ostd_u_obj_id, session
    )
    log.info(
        "Recorded 4MOST submission for '%s': survey='%s', pk_4most=%s.",
        row.transient_name,
        survey_submitted_as,
        pk_4most,
    )
    return row
