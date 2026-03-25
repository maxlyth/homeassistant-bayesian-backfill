"""
Integration tests for backfill_bayesian_sensor.

Calls the live Home Assistant REST API and confirms the service completes
without exceptions by diffing the HA log before and after.

Environment variables:
  HA_URL    Base URL of the HA instance.
            When running inside HA OS: http://supervisor/core
            When running externally:   http://homeassistant.local:8123
  HA_TOKEN  Long-lived access token (or SUPERVISOR_TOKEN inside HA OS)

Run (inside HA OS):
  HA_URL=http://supervisor/core HA_TOKEN=$SUPERVISOR_TOKEN pytest tests/test_integration.py -v

Run (externally):
  HA_URL=http://homeassistant.local:8123 HA_TOKEN=<token> pytest tests/test_integration.py -v
"""
import os
import re
import time
from datetime import datetime, timezone

import pytest
import requests

HA_URL = os.environ.get("HA_URL", "").rstrip("/")
HA_TOKEN = os.environ.get("HA_TOKEN", "")

pytestmark = pytest.mark.integration

# How long to wait for the async pyscript service to finish before reading logs.
# A dry_run over a 1-hour window produces a single row and completes in <1s;
# 5 seconds gives plenty of headroom.
_COMPLETION_WAIT = 5

# ANSI escape codes emitted by the supervisor log endpoint
_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def _skip_if_unconfigured():
    if not HA_URL or not HA_TOKEN:
        pytest.skip("HA_URL and HA_TOKEN environment variables required")


@pytest.fixture(scope="module")
def ha_session():
    _skip_if_unconfigured()
    session = requests.Session()
    session.headers.update(
        {
            "Authorization": f"Bearer {HA_TOKEN}",
            "Content-Type": "application/json",
        }
    )
    return session


def _call_service(session, domain, service, data):
    url = f"{HA_URL}/api/services/{domain}/{service}"
    return session.post(url, json=data, timeout=30)


def _set_log_level(session, logger, level):
    _call_service(session, "logger", "set_level", {logger: level})


def _fetch_log(session):
    """Fetch the HA log. Tries /logs (supervisor proxy) then /api/error_log (direct)."""
    for path in ("/logs", "/api/error_log"):
        resp = session.get(f"{HA_URL}{path}", timeout=10)
        if resp.status_code == 200:
            return _ANSI_RE.sub("", resp.text)
    pytest.fail(f"Could not fetch HA log from {HA_URL}/logs or {HA_URL}/api/error_log")


def _log_lines_after(log_text, since: datetime):
    """Return lines whose leading timestamp is >= since (UTC)."""
    since_str = since.strftime("%Y-%m-%d %H:%M:%S")
    result = []
    for line in log_text.splitlines():
        # Log lines start with "YYYY-MM-DD HH:MM:SS"
        if len(line) >= 19 and line[:19] >= since_str:
            result.append(line)
    return result


class TestBackfillBayesianSensorIntegration:
    def test_service_completes_without_error(self, ha_session):
        """dry_run over a 1-hour window logs a 'finished' completion message and no exceptions."""
        t0 = datetime.now(timezone.utc)

        # No start_offset: dry_run=True triggers the 10-minute safety cap automatically
        resp = _call_service(
            ha_session,
            "pyscript",
            "backfill_bayesian_sensor",
            {
                "target_entity_id": "binary_sensor.*",
                "dry_run": True,
                "debug": True,
            },
        )
        assert resp.status_code == 200, (
            f"Service call rejected with {resp.status_code}: {resp.text}"
        )

        time.sleep(_COMPLETION_WAIT)

        # Filter by timestamp so rolling-buffer churn doesn't drop our lines
        new_lines = _log_lines_after(_fetch_log(ha_session), t0)

        # Confirm the service ran to completion
        finished_lines = [l for l in new_lines if "backfill_bayesian: finished" in l]
        assert finished_lines, (
            f"No 'backfill_bayesian: finished' message found in HA log after {_COMPLETION_WAIT}s. "
            "Service may not have run or pyscript was not reloaded."
        )

        # Confirm no exceptions were raised
        exception_lines = [
            l for l in new_lines if "backfill" in l.lower() and "Exception" in l
        ]
        assert not exception_lines, (
            "Exceptions found in HA log after service call:\n"
            + "\n".join(exception_lines)
        )


    def test_write_history_populates_probability_attribute(self, ha_session):
        """write_history=True over a 2-hour window writes states with probability attribute."""
        _WRITE_WAIT = 20  # real DB write needs more headroom than dry_run
        t0 = datetime.now(timezone.utc)

        resp = _call_service(
            ha_session,
            "pyscript",
            "backfill_bayesian_sensor",
            {
                "target_entity_id": "binary_sensor.max_is_home",
                "start_offset": {"hours": 2},
                "end_offset": {"hours": 0},
                "dry_run": False,
                "debug": True,
                "write_history": True,
            },
        )
        assert resp.status_code == 200, (
            f"Service call rejected with {resp.status_code}: {resp.text}"
        )

        time.sleep(_WRITE_WAIT)

        new_lines = _log_lines_after(_fetch_log(ha_session), t0)

        finished_lines = [l for l in new_lines if "backfill_bayesian: finished" in l]
        assert finished_lines, (
            f"No 'backfill_bayesian: finished' in log after {_WRITE_WAIT}s."
        )

        # Verify state history was written via the HA history API
        from datetime import timedelta
        start_dt = (t0.replace(microsecond=0) - timedelta(hours=2)).isoformat()
        hist_resp = ha_session.get(
            f"{HA_URL}/api/history/period/{start_dt}",
            params={"filter_entity_id": "binary_sensor.max_is_home", "minimal_response": False},
            timeout=15,
        )
        assert hist_resp.status_code == 200, f"History API failed: {hist_resp.status_code}"

        history = hist_resp.json()
        assert history, "History API returned empty result"
        states = history[0]

        prob_values = [
            s["attributes"].get("probability")
            for s in states
            if "attributes" in s and "probability" in s.get("attributes", {})
        ]
        assert prob_values, (
            "No state records with 'probability' attribute found in history. "
            "write_history may not have run or states were not committed."
        )
