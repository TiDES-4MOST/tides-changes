"""Prefect flow: CHANGES Kafka stream → tideschanges_shared → 4MOST.

This flow mirrors the rubin/LSST flow in the ``tides-ingest`` repository but
listens to the CHANGES Kafka topic instead of the LSST one.

Environment variables
---------------------
``TIDES_DB_URL``
    SQLAlchemy PostgreSQL connection string for the tides database.
``CHANGES_KAFKA_BROKER``
    Comma-separated list of Kafka bootstrap servers for the CHANGES stream.
``CHANGES_KAFKA_TOPIC``
    Kafka topic name for incoming CHANGES transients.
``CHANGES_KAFKA_GROUP_ID``
    Consumer group ID (default: ``tides-changes``).
``CHANGES_KAFKA_POLL_TIMEOUT``
    How long (seconds) to block waiting for a Kafka message (default: ``1.0``).
``CHANGES_MAX_MESSAGES``
    Maximum number of messages to process per flow run (default: ``100``).

Dependencies on tides-ingest
-----------------------------
``submit_transients`` is imported from the ``tides_ingest`` package.  If that
package is not installed, you will get an ``ImportError`` at flow invocation
time.  Install it with::

    pip install git+https://github.com/TiDES-4MOST/tides-ingest.git

"""

from __future__ import annotations

import json
import os
from typing import Any

from prefect import flow, get_run_logger

from tides_changes.db import get_session
from tides_changes.tasks.bookkeeping import (
    check_tides_master,
    check_tideschanges_shared,
    determine_survey_name,
    record_changes_trigger,
    record_submission_result,
)

# ---------------------------------------------------------------------------
# Optional import from tides_ingest
# ---------------------------------------------------------------------------
try:
    from tides_ingest.tasks import submit_transients  # type: ignore[import]
except ImportError:
    submit_transients = None  # type: ignore[assignment]


# ---------------------------------------------------------------------------
# Kafka helpers
# ---------------------------------------------------------------------------


def _build_consumer():
    """Build and return a ``confluent_kafka.Consumer`` for the CHANGES topic."""
    from confluent_kafka import Consumer  # local import – optional dep

    broker = os.environ.get("CHANGES_KAFKA_BROKER", "localhost:9092")
    group_id = os.environ.get("CHANGES_KAFKA_GROUP_ID", "tides-changes")
    consumer = Consumer(
        {
            "bootstrap.servers": broker,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    topic = os.environ.get("CHANGES_KAFKA_TOPIC", "changes-transients")
    consumer.subscribe([topic])
    return consumer


def _poll_messages(consumer, max_messages: int, poll_timeout: float) -> list[dict[str, Any]]:
    """Poll *consumer* and return a list of decoded transient dicts."""
    messages = []
    for _ in range(max_messages):
        msg = consumer.poll(timeout=poll_timeout)
        if msg is None:
            break
        if msg.error():
            raise RuntimeError(f"Kafka error: {msg.error()}")
        messages.append(json.loads(msg.value().decode("utf-8")))
        consumer.commit(message=msg)
    return messages


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="changes-ingest")
def changes_ingest_flow(
    max_messages: int | None = None,
    dry_run: bool = False,
) -> list[dict[str, Any]]:
    """Consume the CHANGES Kafka stream and submit transients to 4MOST.

    For each transient message the flow:

    1. Checks ``tideschanges_shared`` – skips the transient if it has already
       been processed (idempotency guard).
    2. Checks ``tides_master`` to determine whether TiDES has already
       submitted this transient (shared vs. CHANGES-only target).
    3. Writes a row to ``tideschanges_shared`` with the appropriate flags.
    4. Determines the survey name to use for the 4MOST submission (the survey
       that triggered *first* wins).
    5. Calls ``submit_transients`` (from tides-ingest) to submit to 4MOST.
    6. Records the 4MOST IDs back into ``tideschanges_shared``.

    Parameters
    ----------
    max_messages:
        Override for the ``CHANGES_MAX_MESSAGES`` environment variable.
    dry_run:
        When ``True`` the flow skips the actual 4MOST submission but still
        performs all database writes.  Useful for testing.

    Returns
    -------
    list[dict]
        A list of result dicts (one per processed transient) containing
        ``transient_name``, ``is_shared``, ``survey_submitted_as``,
        ``pk_4most``, and ``ostd_u_obj_id``.
    """
    logger = get_run_logger()

    if submit_transients is None and not dry_run:
        raise RuntimeError(
            "tides_ingest is not installed.  Install it with:\n"
            "  pip install git+https://github.com/TiDES-4MOST/tides-ingest.git\n"
            "or set dry_run=True to skip 4MOST submission."
        )

    _max = max_messages or int(os.environ.get("CHANGES_MAX_MESSAGES", "100"))
    _poll_timeout = float(os.environ.get("CHANGES_KAFKA_POLL_TIMEOUT", "1.0"))

    consumer = _build_consumer()
    logger.info("Polling up to %d messages from CHANGES Kafka topic.", _max)

    try:
        transients = _poll_messages(consumer, _max, _poll_timeout)
    finally:
        consumer.close()

    logger.info("Received %d transient messages.", len(transients))
    results: list[dict[str, Any]] = []

    with get_session() as session:
        for transient in transients:
            result = _process_transient(
                transient=transient,
                session=session,
                dry_run=dry_run,
            )
            if result is not None:
                results.append(result)

    logger.info("Flow complete. Processed %d transients.", len(results))
    return results


def _process_transient(
    transient: dict[str, Any],
    session,
    dry_run: bool,
) -> dict[str, Any] | None:
    """Process a single CHANGES transient through the full pipeline.

    Returns a summary dict, or ``None`` if the transient was skipped.
    """
    logger = get_run_logger()
    name = transient.get("transient_name") or transient.get("objectId")
    if not name:
        logger.warning("Transient message missing 'transient_name'/'objectId'; skipping.")
        return None

    # Normalise to a consistent key
    transient = {**transient, "transient_name": name}

    # ------------------------------------------------------------------
    # 1. Idempotency guard: skip if already processed by this flow
    # ------------------------------------------------------------------
    existing = check_tideschanges_shared(name, session)
    if existing is not None and existing.changes_triggered:
        logger.info("'%s' already processed by CHANGES flow; skipping.", name)
        return None

    # ------------------------------------------------------------------
    # 2. Check tides_master (has TiDES already submitted this?)
    # ------------------------------------------------------------------
    tides_row = check_tides_master(name, session)
    is_shared = tides_row is not None

    # ------------------------------------------------------------------
    # 3. Record in tideschanges_shared
    # ------------------------------------------------------------------
    shared_row = record_changes_trigger(transient, is_shared, session)

    # ------------------------------------------------------------------
    # 4. Determine the survey name for 4MOST submission
    # ------------------------------------------------------------------
    survey_name = determine_survey_name(shared_row)

    # ------------------------------------------------------------------
    # 5. Submit to 4MOST (unless this is a shared target where TiDES
    #    already submitted it – in that case 4MOST already has it)
    # ------------------------------------------------------------------
    pk_4most: int | None = None
    ostd_u_obj_id: str | None = None

    if is_shared:
        # TiDES already submitted to 4MOST; inherit the 4MOST IDs from
        # tides_master and do not re-submit.
        pk_4most = tides_row.pk_4most  # type: ignore[union-attr]
        ostd_u_obj_id = tides_row.ostd_u_obj_id  # type: ignore[union-attr]
        logger.info(
            "'%s' is a shared target; inheriting 4MOST IDs from tides_master "
            "(pk_4most=%s, ostd_u_obj_id=%s). No re-submission.",
            name,
            pk_4most,
            ostd_u_obj_id,
        )
    else:
        # Pure CHANGES target – submit to 4MOST under the CHANGES survey name
        if not dry_run:
            submission_result = submit_transients(
                transients=[transient],
                survey=survey_name,
            )
            pk_4most = submission_result.get("pk_4most")
            ostd_u_obj_id = submission_result.get("ostd_u_obj_id")
        else:
            logger.info("dry_run=True; skipping 4MOST submission for '%s'.", name)

    # ------------------------------------------------------------------
    # 6. Record the submission outcome
    # ------------------------------------------------------------------
    record_submission_result(
        shared_row=shared_row,
        survey_submitted_as=survey_name,
        pk_4most=pk_4most,
        ostd_u_obj_id=ostd_u_obj_id,
        session=session,
    )

    return {
        "transient_name": name,
        "is_shared": is_shared,
        "survey_submitted_as": survey_name,
        "pk_4most": pk_4most,
        "ostd_u_obj_id": ostd_u_obj_id,
    }
