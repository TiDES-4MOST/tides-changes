"""Unit tests for tides_changes.tasks.bookkeeping.

These tests use an in-memory SQLite database so they run without a live
PostgreSQL server.  The ``schema`` argument used in the model definitions is
not supported by SQLite, so we patch it out before the engine is created.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from tides_changes.models import Base, TidesChangesShared, TidesMaster
from tides_changes.tasks.bookkeeping import (
    query_tides_master,
    query_tideschanges_shared,
    resolve_survey_name,
    upsert_changes_trigger,
    upsert_tides_trigger,
    write_submission_result,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _strip_schemas():
    """SQLite does not support schemas – remove them from table metadata."""
    original_schemas = {}
    for name, table in Base.metadata.tables.items():
        original_schemas[name] = table.schema
        table.schema = None
    yield
    for name, table in Base.metadata.tables.items():
        table.schema = original_schemas[name]


@pytest.fixture()
def engine(_strip_schemas):
    eng = create_engine("sqlite:///:memory:", echo=False)
    Base.metadata.create_all(eng)
    yield eng
    Base.metadata.drop_all(eng)


@pytest.fixture()
def session(engine):
    with Session(engine) as s:
        yield s


# ---------------------------------------------------------------------------
# query_tides_master
# ---------------------------------------------------------------------------


class TestQueryTidesMaster:
    def test_returns_none_when_absent(self, session):
        assert query_tides_master("AT2024abc", session) is None

    def test_returns_row_when_present(self, session):
        session.add(TidesMaster(transient_name="AT2024abc", ra=10.0, dec=-5.0))
        session.commit()
        row = query_tides_master("AT2024abc", session)
        assert row is not None
        assert row.transient_name == "AT2024abc"


# ---------------------------------------------------------------------------
# query_tideschanges_shared
# ---------------------------------------------------------------------------


class TestQueryTidesChangesShared:
    def test_returns_none_when_absent(self, session):
        assert query_tideschanges_shared("AT2024xyz", session) is None

    def test_returns_row_when_present(self, session):
        session.add(
            TidesChangesShared(
                transient_name="AT2024xyz",
                changes_triggered=True,
                changes_triggered_at=datetime.now(timezone.utc),
            )
        )
        session.commit()
        row = query_tideschanges_shared("AT2024xyz", session)
        assert row is not None
        assert row.transient_name == "AT2024xyz"


# ---------------------------------------------------------------------------
# upsert_changes_trigger
# ---------------------------------------------------------------------------


class TestUpsertChangesTrigger:
    def test_creates_changes_only_row(self, session):
        transient = {"transient_name": "AT2024new", "ra": 12.3, "dec": -4.5}
        row = upsert_changes_trigger(transient, is_shared=False, session=session)

        assert row.transient_name == "AT2024new"
        assert row.changes_triggered is True
        assert row.tides_triggered is False
        assert row.is_shared is False
        assert row.first_trigger == "changes"

    def test_creates_shared_row_when_in_tides_master(self, session):
        transient = {"transient_name": "AT2024shared", "ra": 20.0, "dec": -10.0}
        row = upsert_changes_trigger(transient, is_shared=True, session=session)

        assert row.is_shared is True
        assert row.tides_triggered is True
        assert row.changes_triggered is True
        assert row.first_trigger == "tides"

    def test_updates_existing_row(self, session):
        session.add(
            TidesChangesShared(
                transient_name="AT2024upd",
                tides_triggered=True,
                tides_triggered_at=datetime.now(timezone.utc),
                first_trigger="tides",
            )
        )
        session.commit()

        row = upsert_changes_trigger(
            {"transient_name": "AT2024upd"}, is_shared=True, session=session
        )
        assert row.changes_triggered is True
        assert row.is_shared is True


# ---------------------------------------------------------------------------
# upsert_tides_trigger
# ---------------------------------------------------------------------------


class TestUpsertTidesTrigger:
    def test_returns_none_when_no_row(self, session):
        assert upsert_tides_trigger("AT2024ghost", session) is None

    def test_marks_tides_triggered(self, session):
        session.add(
            TidesChangesShared(
                transient_name="AT2024tides",
                changes_triggered=True,
                changes_triggered_at=datetime.now(timezone.utc),
                first_trigger="changes",
            )
        )
        session.commit()

        updated = upsert_tides_trigger("AT2024tides", session)
        assert updated.tides_triggered is True
        assert updated.is_shared is True


# ---------------------------------------------------------------------------
# resolve_survey_name
# ---------------------------------------------------------------------------


class TestResolveSurveyName:
    def _row(self, **kwargs):
        return TidesChangesShared(**kwargs)

    def test_changes_only(self):
        row = self._row(changes_triggered=True, tides_triggered=False, is_shared=False)
        assert resolve_survey_name(row) == "CHANGES"

    def test_tides_only(self):
        row = self._row(changes_triggered=False, tides_triggered=True, is_shared=False)
        assert resolve_survey_name(row) == "TiDES"

    def test_shared_tides_first(self):
        row = self._row(
            changes_triggered=True,
            tides_triggered=True,
            is_shared=True,
            first_trigger="tides",
        )
        assert resolve_survey_name(row) == "TiDES"

    def test_shared_changes_first(self):
        row = self._row(
            changes_triggered=True,
            tides_triggered=True,
            is_shared=True,
            first_trigger="changes",
        )
        assert resolve_survey_name(row) == "CHANGES"


# ---------------------------------------------------------------------------
# write_submission_result
# ---------------------------------------------------------------------------


class TestWriteSubmissionResult:
    def test_persists_4most_ids(self, session):
        row = TidesChangesShared(transient_name="AT2024sub")
        session.add(row)
        session.commit()

        updated = write_submission_result(
            row, "CHANGES", pk_4most=42, ostd_u_obj_id="OSTD-99", session=session
        )
        assert updated.pk_4most == 42
        assert updated.ostd_u_obj_id == "OSTD-99"
        assert updated.survey_submitted_as == "CHANGES"
        assert updated.submitted_at is not None
