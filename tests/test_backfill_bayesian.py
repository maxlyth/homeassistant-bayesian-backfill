"""
Tests for backfill_bayesian.

core.py is pure Python — imported directly, no pyscript stubs needed.
The __init__.py (@service orchestration) requires the full HA async runtime
and is tested via tests/test_integration.py against a live HA instance.
"""
import asyncio
import builtins
import json
import math
import os
import sqlite3
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pytest

# Import core.py directly by path to avoid loading __init__.py (which has
# pyscript-specific syntax that CPython can't parse without preprocessing)
import importlib.util
_CORE_PATH = Path(__file__).parent.parent / "backfill_bayesian" / "core.py"
_spec = importlib.util.spec_from_file_location("backfill_bayesian_core", _CORE_PATH)
core = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(core)


@pytest.fixture(scope="module")
def m():
    """The core module — pure Python, no pyscript dependencies."""
    return core


def _load_init_module():
    """Load backfill_bayesian/__init__.py with pyscript syntax normalised for CPython.

    The @service decorator allows plain `def` with `await` inside — CPython
    rejects this. We rewrite to `async def` before compiling.
    """
    import re as _re
    import types

    init_path = Path(__file__).parent.parent / "backfill_bayesian" / "__init__.py"
    source = init_path.read_text()
    source = _re.sub(r"@service(?:\([^)]*\))?\s*\ndef\s+", "async def ", source)
    # Strip pyscript-specific lines that won't work in standard Python
    source = source.replace(
        "from .core import", "from backfill_bayesian_core import"
    )
    source = _re.sub(r"# __file__.*?\n_CORE_PY_PATH = .*?\n", "", source)
    source = _re.sub(
        r"import importlib\.util as _ilu\n.*?_spec\.loader\.exec_module\(_raw_core\)\n",
        "_raw_core = _ilu_placeholder\n",
        source, flags=_re.DOTALL,
    )

    mod = types.ModuleType("backfill_bayesian_init")
    mod.__file__ = str(init_path)
    import builtins as _builtins
    for _name in ("pyscript_executor", "service", "log", "task", "hass"):
        setattr(mod, _name, getattr(_builtins, _name, lambda fn: fn))
    # In tests, core is a plain Python module (not pyscript-wrapped), so
    # _raw_core and the from-import names are all the same real functions.
    import sys as _sys
    _sys.modules["backfill_bayesian_core"] = core
    mod._ilu_placeholder = core  # _raw_core = core
    mod._WRITE_BATCH_SIZE = core._WRITE_BATCH_SIZE
    mod._BACKFILL_ORIGIN_IDX = core._BACKFILL_ORIGIN_IDX
    exec(compile(source, str(init_path), "exec"), mod.__dict__)
    _sys.modules.pop("backfill_bayesian_core", None)
    return mod


# ===========================================================================
# _solar_elevation
# ===========================================================================


class TestSolarElevation:
    def test_midnight_is_below_horizon(self, m):
        # London (51.5°N, 0°W), 2024-06-21 00:00 UTC — sun well below horizon
        ts = datetime(2024, 6, 21, 0, 0, tzinfo=timezone.utc).timestamp()
        elev = m.solar_elevation(ts, 51.5, 0.0)
        assert elev < 0, f"Expected negative at midnight, got {elev:.2f}°"

    def test_noon_is_above_horizon(self, m):
        # London, 2024-06-21 ~12:00 UTC — sun well above horizon (summer)
        ts = datetime(2024, 6, 21, 12, 0, tzinfo=timezone.utc).timestamp()
        elev = m.solar_elevation(ts, 51.5, 0.0)
        assert elev > 50, f"Expected >50° at summer noon London, got {elev:.2f}°"

    def test_winter_noon_lower_than_summer(self, m):
        summer_ts = datetime(2024, 6, 21, 12, 0, tzinfo=timezone.utc).timestamp()
        winter_ts = datetime(2024, 12, 21, 12, 0, tzinfo=timezone.utc).timestamp()
        summer = m.solar_elevation(summer_ts, 51.5, 0.0)
        winter = m.solar_elevation(winter_ts, 51.5, 0.0)
        assert summer > winter, f"Summer ({summer:.1f}°) should exceed winter ({winter:.1f}°)"

    def test_equatorial_noon_near_90(self, m):
        # Near equator on equinox at solar noon → elevation ≈ 90°
        ts = datetime(2024, 3, 20, 12, 0, tzinfo=timezone.utc).timestamp()
        elev = m.solar_elevation(ts, 0.0, 0.0)
        assert 80 < elev <= 90, f"Expected near 90° at equatorial equinox noon, got {elev:.2f}°"

    def test_returns_float(self, m):
        ts = datetime(2024, 6, 21, 6, 0, tzinfo=timezone.utc).timestamp()
        result = m.solar_elevation(ts, 51.5, -0.23)
        assert isinstance(result, float)


# ===========================================================================
# _get_state_at / _get_attr_at
# ===========================================================================


@pytest.fixture
def timeline():
    """A simple sorted timeline with ts/state/attrs lists."""
    return {
        "ts":    [1000.0, 2000.0, 3000.0],
        "state": ["off",  "on",   "off"],
        "attrs": [None,    None,   None],
    }


class TestGetStateAt:
    def test_before_first_entry_returns_none(self, m, timeline):
        assert m.get_state_at(timeline, 500.0) is None

    def test_exactly_at_first_entry(self, m, timeline):
        assert m.get_state_at(timeline, 1000.0) == "off"

    def test_between_entries_returns_previous(self, m, timeline):
        assert m.get_state_at(timeline, 1500.0) == "off"
        assert m.get_state_at(timeline, 2500.0) == "on"

    def test_exactly_at_later_entry(self, m, timeline):
        assert m.get_state_at(timeline, 2000.0) == "on"

    def test_after_last_entry(self, m, timeline):
        assert m.get_state_at(timeline, 9999.0) == "off"

    def test_empty_timeline_returns_none(self, m):
        assert m.get_state_at({"ts": [], "state": [], "attrs": []}, 1000.0) is None

    def test_none_timeline_returns_none(self, m):
        assert m.get_state_at(None, 1000.0) is None


class TestGetAttrAt:
    def test_no_attrs_returns_none(self, m, timeline):
        assert m.get_attr_at(timeline, 2000.0, "brightness") is None

    def test_returns_attribute_value(self, m):
        tl = {
            "ts":    [1000.0, 2000.0],
            "state": ["on",   "on"],
            "attrs": ['{"brightness": 128}', '{"brightness": 200}'],
        }
        assert m.get_attr_at(tl, 1000.0, "brightness") == 128
        assert m.get_attr_at(tl, 1500.0, "brightness") == 128  # forward-fill
        assert m.get_attr_at(tl, 2000.0, "brightness") == 200

    def test_missing_key_returns_none(self, m):
        tl = {
            "ts":    [1000.0],
            "state": ["on"],
            "attrs": ['{"other_key": 42}'],
        }
        assert m.get_attr_at(tl, 1000.0, "brightness") is None

    def test_invalid_json_returns_none(self, m):
        tl = {
            "ts":    [1000.0],
            "state": ["on"],
            "attrs": ["not valid json"],
        }
        assert m.get_attr_at(tl, 1000.0, "brightness") is None


# ===========================================================================
# _compute_bayesian_probability
# ===========================================================================

def _obs(p_true, p_false):
    return {"prob_given_true": p_true, "prob_given_false": p_false}


class TestComputeBayesianProbability:
    def test_single_active_observation_raises_probability(self, m):
        obs = [_obs(0.9, 0.1)]
        # P = (0.5 * 0.9) / (0.5 * 0.9 + 0.5 * 0.1) = 0.9
        result = m.compute_bayesian_probability(0.5, obs, [True])
        assert abs(result - 0.9) < 1e-4

    def test_single_inactive_observation_lowers_probability(self, m):
        obs = [_obs(0.9, 0.1)]
        # P = (0.5 * 0.1) / (0.5 * 0.1 + 0.5 * 0.9) = 0.1
        result = m.compute_bayesian_probability(0.5, obs, [False])
        assert abs(result - 0.1) < 1e-4

    def test_skipped_observations_return_prior(self, m):
        obs = [_obs(0.9, 0.1), _obs(0.8, 0.2)]
        result = m.compute_bayesian_probability(0.7, obs, [None, None])
        assert abs(result - 0.7) < 1e-4

    def test_multiple_observations_compound(self, m):
        obs = [_obs(0.9, 0.1), _obs(0.8, 0.2)]
        result = m.compute_bayesian_probability(0.5, obs, [True, True])
        # Both active → probability should be very high
        assert result > 0.9

    def test_result_clamped_to_min(self, m):
        # Extreme: prior near 0, strong evidence against
        obs = [_obs(0.01, 0.99)] * 10
        result = m.compute_bayesian_probability(0.01, obs, [False] * 10)
        assert result >= 0.0001

    def test_result_clamped_to_max(self, m):
        obs = [_obs(0.99, 0.01)] * 10
        result = m.compute_bayesian_probability(0.99, obs, [True] * 10)
        assert result <= 0.9999

    def test_zero_denom_skips_observation(self, m):
        # p_true=1, p_false=1 → denom=0 for active → skip
        obs = [_obs(1.0, 1.0)]
        result = m.compute_bayesian_probability(0.5, obs, [True])
        assert abs(result - 0.5) < 1e-4

    def test_mixed_active_and_skipped(self, m):
        obs = [_obs(0.9, 0.1), _obs(0.8, 0.2)]
        # Only first observation counts
        result_mixed = m.compute_bayesian_probability(0.5, obs, [True, None])
        result_single = m.compute_bayesian_probability(0.5, [obs[0]], [True])
        assert abs(result_mixed - result_single) < 1e-6


# ===========================================================================
# _evaluate_observation
# ===========================================================================


class TestEvaluateObservation:
    """Tests for state/numeric_state/template dispatch."""

    def _ctx(self, ts=1000.0):
        return {"ts": float(ts)}

    def _tl(self, ts, state, attrs=None):
        return {
            "ts": [float(t) for t in ts],
            "state": list(state),
            "attrs": [attrs] * len(ts) if not isinstance(attrs, list) else attrs,
        }

    # --- state platform ---

    def test_state_active_when_matches(self, m):
        tl = {"binary_sensor.foo": self._tl([500.0], ["on"])}
        obs = {"platform": "state", "entity_id": "binary_sensor.foo", "to_state": "on"}
        ctx = self._ctx(1000.0)
        assert m.evaluate_observation(obs, tl, {}, 0, ctx) is True

    def test_state_inactive_when_no_match(self, m):
        tl = {"binary_sensor.foo": self._tl([500.0], ["off"])}
        obs = {"platform": "state", "entity_id": "binary_sensor.foo", "to_state": "on"}
        assert m.evaluate_observation(obs, tl, {}, 0, self._ctx()) is False

    def test_state_skip_when_no_history(self, m):
        # Timeline exists but no entry before ts=100
        tl = {"binary_sensor.foo": self._tl([500.0], ["on"])}
        obs = {"platform": "state", "entity_id": "binary_sensor.foo", "to_state": "on"}
        assert m.evaluate_observation(obs, tl, {}, 0, self._ctx(50.0)) is None

    def test_state_skip_when_entity_missing(self, m):
        obs = {"platform": "state", "entity_id": "binary_sensor.missing", "to_state": "on"}
        assert m.evaluate_observation(obs, {}, {}, 0, self._ctx()) is None

    # --- numeric_state platform ---

    def test_numeric_below_threshold_active(self, m):
        tl = {"sensor.temp": self._tl([500.0], ["18.5"])}
        obs = {"platform": "numeric_state", "entity_id": "sensor.temp", "below": 20.0}
        assert m.evaluate_observation(obs, tl, {}, 0, self._ctx()) is True

    def test_numeric_above_threshold_active(self, m):
        tl = {"sensor.temp": self._tl([500.0], ["25.0"])}
        obs = {"platform": "numeric_state", "entity_id": "sensor.temp", "above": 20.0}
        assert m.evaluate_observation(obs, tl, {}, 0, self._ctx()) is True

    def test_numeric_above_and_below_active(self, m):
        tl = {"sensor.temp": self._tl([500.0], ["22.0"])}
        obs = {"platform": "numeric_state", "entity_id": "sensor.temp",
               "above": 20.0, "below": 25.0}
        assert m.evaluate_observation(obs, tl, {}, 0, self._ctx()) is True

    def test_numeric_outside_range_inactive(self, m):
        tl = {"sensor.temp": self._tl([500.0], ["30.0"])}
        obs = {"platform": "numeric_state", "entity_id": "sensor.temp",
               "above": 20.0, "below": 25.0}
        assert m.evaluate_observation(obs, tl, {}, 0, self._ctx()) is False

    def test_numeric_unavailable_state_skipped(self, m):
        tl = {"sensor.temp": self._tl([500.0], ["unavailable"])}
        obs = {"platform": "numeric_state", "entity_id": "sensor.temp", "below": 25.0}
        assert m.evaluate_observation(obs, tl, {}, 0, self._ctx()) is None

    # --- template platform ---

    def test_template_true_result(self, m):
        jinja2 = pytest.importorskip("jinja2")
        env = jinja2.Environment()
        tpl = env.from_string("{{ True }}")
        obs = {"platform": "template", "value_template": "{{ True }}"}
        assert m.evaluate_observation(obs, {}, {0: tpl}, 0, self._ctx()) is True

    def test_template_false_result(self, m):
        jinja2 = pytest.importorskip("jinja2")
        env = jinja2.Environment()
        tpl = env.from_string("{{ False }}")
        obs = {"platform": "template", "value_template": "{{ False }}"}
        assert m.evaluate_observation(obs, {}, {0: tpl}, 0, self._ctx()) is False

    def test_template_missing_returns_false(self, m):
        obs = {"platform": "template", "value_template": "{{ True }}"}
        # No compiled template at index 0 → returns False (conservative)
        assert m.evaluate_observation(obs, {}, {}, 0, self._ctx()) is False

    def test_unknown_platform_returns_none(self, m):
        obs = {"platform": "mystery"}
        assert m.evaluate_observation(obs, {}, {}, 0, self._ctx()) is None


# ===========================================================================
# _build_jinja2_env — states(), state_attr(), now() mocks
# ===========================================================================


class TestBuildJinja2Env:
    def test_states_returns_known_state(self, m):
        pytest.importorskip("jinja2")
        tl = {
            "sensor.foo": {
                "ts": [500.0],
                "state": ["on"],
                "attrs": [None],
            }
        }
        env, ctx = m.build_jinja2_env(tl, 51.5, -0.23)
        ctx["ts"] = 1000.0
        tpl = env.from_string("{{ states('sensor.foo') }}")
        assert tpl.render() == "on"

    def test_states_returns_unknown_for_missing_entity(self, m):
        pytest.importorskip("jinja2")
        env, ctx = m.build_jinja2_env({}, 51.5, -0.23)
        ctx["ts"] = 1000.0
        tpl = env.from_string("{{ states('sensor.missing') }}")
        assert tpl.render() == "unknown"

    def test_now_reflects_historical_timestamp(self, m):
        pytest.importorskip("jinja2")
        env, ctx = m.build_jinja2_env({}, 51.5, -0.23)
        ts = datetime(2024, 6, 21, 14, 30, tzinfo=timezone.utc).timestamp()
        ctx["ts"] = ts
        tpl = env.from_string("{{ now().hour }}")
        assert tpl.render() == "14"

    def test_state_attr_sun_elevation_is_numeric(self, m):
        pytest.importorskip("jinja2")
        env, ctx = m.build_jinja2_env({}, 51.5, -0.23)
        ts = datetime(2024, 6, 21, 12, 0, tzinfo=timezone.utc).timestamp()
        ctx["ts"] = ts
        tpl = env.from_string("{{ state_attr('sun.sun', 'elevation') | float | round(0) }}")
        result = float(tpl.render())
        # Solar elevation at London summer noon should be well above 5°
        assert result > 5

    def test_time_dependent_template_uses_historical_hour(self, m):
        pytest.importorskip("jinja2")
        env, ctx = m.build_jinja2_env({}, 51.5, -0.23)
        # Set ctx to a known hour (e.g. 22:00 UTC)
        ts = datetime(2024, 1, 15, 22, 0, tzinfo=timezone.utc).timestamp()
        ctx["ts"] = ts
        tpl = env.from_string("{{ now().hour >= 22 or now().hour < 6 }}")
        assert tpl.render().strip().lower() == "true"


# ===========================================================================
# SQLite integration: _load_state_timelines
# ===========================================================================


@pytest.fixture
def states_db(tmp_path):
    """
    In-memory SQLite DB with states_meta + states + state_attributes tables
    and a few rows for two entities.
    """
    db_path = str(tmp_path / "test.db")
    conn = sqlite3.connect(db_path)
    conn.executescript("""
        CREATE TABLE states_meta (
            metadata_id INTEGER PRIMARY KEY,
            entity_id   TEXT NOT NULL
        );
        CREATE TABLE state_attributes (
            attributes_id INTEGER PRIMARY KEY,
            hash          TEXT,
            shared_attrs  TEXT
        );
        CREATE TABLE states (
            state_id       INTEGER PRIMARY KEY,
            metadata_id    INTEGER,
            state          TEXT,
            last_updated_ts REAL,
            attributes_id  INTEGER
        );
        INSERT INTO states_meta VALUES (1, 'sensor.foo');
        INSERT INTO states_meta VALUES (2, 'sensor.bar');
        INSERT INTO state_attributes VALUES (10, 'h1', '{"brightness": 100}');
        INSERT INTO state_attributes VALUES (11, 'h2', '{"brightness": 200}');
        -- sensor.foo: off at t=1000, on at t=2000
        INSERT INTO states VALUES (1, 1, 'off', 1000.0, 10);
        INSERT INTO states VALUES (2, 1, 'on',  2000.0, 11);
        -- sensor.bar: 15.5 at t=1500, 22.0 at t=3000
        INSERT INTO states VALUES (3, 2, '15.5', 1500.0, NULL);
        INSERT INTO states VALUES (4, 2, '22.0', 3000.0, NULL);
    """)
    conn.commit()
    conn.close()
    return db_path


class TestLoadStateTimelines:
    def test_loads_correct_entity_ids(self, m, states_db):
        result = m.load_state_timelines(
            ["sensor.foo", "sensor.bar"], 500.0, 4000.0, False, states_db
        )
        assert "sensor.foo" in result
        assert "sensor.bar" in result

    def test_state_values_correct(self, m, states_db):
        result = m.load_state_timelines(
            ["sensor.foo"], 500.0, 4000.0, False, states_db
        )
        assert result["sensor.foo"]["state"] == ["off", "on"]

    def test_timestamps_sorted(self, m, states_db):
        result = m.load_state_timelines(
            ["sensor.foo"], 500.0, 4000.0, False, states_db
        )
        ts = result["sensor.foo"]["ts"]
        assert ts == sorted(ts)

    def test_1day_lookback_seed_included(self, m, states_db):
        # start_ts=1800 → lookback=1800-86400 < 1000, so the t=1000 entry should be included
        result = m.load_state_timelines(
            ["sensor.foo"], 1800.0, 4000.0, False, states_db
        )
        assert 1000.0 in result["sensor.foo"]["ts"]

    def test_attrs_loaded_when_requested(self, m, states_db):
        result = m.load_state_timelines(
            ["sensor.foo"], 500.0, 4000.0, True, states_db
        )
        attrs = result["sensor.foo"]["attrs"]
        # First entry has brightness=100
        assert json.loads(attrs[0])["brightness"] == 100

    def test_attrs_none_when_not_requested(self, m, states_db):
        result = m.load_state_timelines(
            ["sensor.foo"], 500.0, 4000.0, False, states_db
        )
        assert all(a is None for a in result["sensor.foo"]["attrs"])

    def test_missing_entity_not_in_result(self, m, states_db):
        result = m.load_state_timelines(
            ["sensor.missing"], 500.0, 4000.0, False, states_db
        )
        assert "sensor.missing" not in result

    def test_forward_fill_roundtrip(self, m, states_db):
        """Load timeline and verify get_state_at forward-fill is correct."""
        result = m.load_state_timelines(
            ["sensor.foo"], 500.0, 4000.0, False, states_db
        )
        tl = result["sensor.foo"]
        assert m.get_state_at(tl, 999.0) is None      # before first entry
        assert m.get_state_at(tl, 1000.0) == "off"    # exactly first
        assert m.get_state_at(tl, 1500.0) == "off"    # between entries
        assert m.get_state_at(tl, 2000.0) == "on"     # exactly second
        assert m.get_state_at(tl, 9999.0) == "on"     # after last


# ===========================================================================
# _load_bayesian_config — YAML discovery
# ===========================================================================


@pytest.fixture
def bayesian_yaml_dir(tmp_path):
    """Minimal config_dir with entity registry and one YAML bayesian sensor."""
    # Entity registry entry (YAML-defined — config_entry_id is null)
    registry = {
        "data": {
            "entities": [
                {
                    "entity_id": "binary_sensor.my_test_sensor",
                    "platform": "bayesian",
                    "config_entry_id": None,
                    "unique_id": "bayesian-my_test_sensor",
                }
            ]
        }
    }
    storage = tmp_path / ".storage"
    storage.mkdir()
    (storage / "core.entity_registry").write_text(json.dumps(registry))

    # YAML file with the bayesian sensor
    yaml_content = """
- platform: bayesian
  name: "My Test Sensor"
  unique_id: my_test_sensor
  prior: 0.3
  probability_threshold: 0.7
  observations:
    - platform: state
      entity_id: binary_sensor.motion
      to_state: "on"
      prob_given_true: 0.9
      prob_given_false: 0.1
"""
    (tmp_path / "binary_sensor.yaml").write_text(yaml_content)
    return tmp_path


class TestLoadBayesianConfig:
    def test_finds_yaml_sensor_by_unique_id(self, m, bayesian_yaml_dir):
        cfg = m.load_bayesian_config(
            "binary_sensor.my_test_sensor", str(bayesian_yaml_dir)
        )
        assert float(cfg["prior"]) == pytest.approx(0.3)
        assert len(cfg["observations"]) == 1
        assert cfg["observations"][0]["entity_id"] == "binary_sensor.motion"

    def test_raises_for_unknown_entity(self, m, bayesian_yaml_dir):
        with pytest.raises(ValueError, match="not found in entity registry"):
            m.load_bayesian_config(
                "binary_sensor.does_not_exist", str(bayesian_yaml_dir)
            )

    def test_raises_when_yaml_not_found(self, m, tmp_path):
        # Registry has the entity but no YAML file contains its config
        registry = {
            "data": {
                "entities": [
                    {
                        "entity_id": "binary_sensor.orphan",
                        "platform": "bayesian",
                        "config_entry_id": None,
                        "unique_id": "bayesian-orphan",
                    }
                ]
            }
        }
        storage = tmp_path / ".storage"
        storage.mkdir()
        (storage / "core.entity_registry").write_text(json.dumps(registry))
        with pytest.raises(ValueError, match="not found"):
            m.load_bayesian_config("binary_sensor.orphan", str(tmp_path))

    def test_finds_sensor_in_nested_package_yaml(self, m, tmp_path):
        """Sensors in package files live under a binary_sensor: key."""
        registry = {
            "data": {
                "entities": [
                    {
                        "entity_id": "binary_sensor.package_sensor",
                        "platform": "bayesian",
                        "config_entry_id": None,
                        "unique_id": "bayesian-package_sensor",
                    }
                ]
            }
        }
        storage = tmp_path / ".storage"
        storage.mkdir()
        (storage / "core.entity_registry").write_text(json.dumps(registry))

        # Nested package format
        yaml_content = """
input_boolean:
  some_helper:
    name: "Helper"
binary_sensor:
  - platform: bayesian
    name: "Package Sensor"
    unique_id: package_sensor
    prior: 0.5
    probability_threshold: 0.6
    observations: []
"""
        packages = tmp_path / "packages"
        packages.mkdir()
        (packages / "activities.yaml").write_text(yaml_content)

        cfg = m.load_bayesian_config("binary_sensor.package_sensor", str(tmp_path))
        assert float(cfg["prior"]) == pytest.approx(0.5)

    def test_loads_ui_sensor_from_config_entries(self, m, tmp_path):
        """UI-created sensors come from core.config_entries, not YAML."""
        registry = {
            "data": {
                "entities": [
                    {
                        "entity_id": "binary_sensor.ui_sensor",
                        "platform": "bayesian",
                        "config_entry_id": "entry-abc123",
                        "unique_id": "some-unique-id",
                    }
                ]
            }
        }
        config_entries = {
            "data": {
                "entries": [
                    {
                        "entry_id": "entry-abc123",
                        "domain": "bayesian",
                        "data": {
                            "prior": 0.6,
                            "probability_threshold": 0.75,
                            "observations": [],
                        },
                    }
                ]
            }
        }
        storage = tmp_path / ".storage"
        storage.mkdir()
        (storage / "core.entity_registry").write_text(json.dumps(registry))
        (storage / "core.config_entries").write_text(json.dumps(config_entries))

        cfg = m.load_bayesian_config("binary_sensor.ui_sensor", str(tmp_path))
        assert float(cfg["prior"]) == pytest.approx(0.6)


# ===========================================================================
# _get_bayesian_entity_ids — glob expansion
# ===========================================================================


@pytest.fixture
def registry_dir(tmp_path):
    registry = {
        "data": {
            "entities": [
                {"entity_id": "binary_sensor.bathroom_has_light", "platform": "bayesian"},
                {"entity_id": "binary_sensor.bathroom_wants_lights", "platform": "bayesian"},
                {"entity_id": "binary_sensor.lounge_tv_watching", "platform": "bayesian"},
                {"entity_id": "binary_sensor.max_is_home", "platform": "bayesian"},
                {"entity_id": "sensor.not_bayesian", "platform": "template"},
            ]
        }
    }
    storage = tmp_path / ".storage"
    storage.mkdir()
    (storage / "core.entity_registry").write_text(json.dumps(registry))
    return tmp_path


class TestGetBayesianEntityIds:
    def test_exact_match_found(self, m, registry_dir):
        result = m.get_bayesian_entity_ids(
            "binary_sensor.bathroom_has_light", str(registry_dir)
        )
        assert result == ["binary_sensor.bathroom_has_light"]

    def test_exact_match_non_bayesian_returns_empty(self, m, registry_dir):
        result = m.get_bayesian_entity_ids("sensor.not_bayesian", str(registry_dir))
        assert result == []

    def test_wildcard_matches_all_bayesian(self, m, registry_dir):
        result = m.get_bayesian_entity_ids("binary_sensor.*", str(registry_dir))
        assert len(result) == 4
        assert "sensor.not_bayesian" not in result

    def test_prefix_glob_matches_bathroom_only(self, m, registry_dir):
        result = m.get_bayesian_entity_ids("binary_sensor.bathroom_*", str(registry_dir))
        assert len(result) == 2
        assert all("bathroom" in eid for eid in result)

    def test_no_match_returns_empty(self, m, registry_dir):
        result = m.get_bayesian_entity_ids("binary_sensor.nonexistent_*", str(registry_dir))
        assert result == []


# ===========================================================================
# _load_location
# ===========================================================================


class TestLoadLocation:
    def test_returns_latitude_and_longitude(self, m, tmp_path):
        storage = tmp_path / ".storage"
        storage.mkdir()
        config = {"data": {"latitude": 48.85, "longitude": 2.35}}
        (storage / "core.config").write_text(json.dumps(config))
        lat, lon = m.load_location(str(tmp_path))
        assert lat == pytest.approx(48.85)
        assert lon == pytest.approx(2.35)

    def test_defaults_to_zero_when_keys_absent(self, m, tmp_path):
        storage = tmp_path / ".storage"
        storage.mkdir()
        (storage / "core.config").write_text(json.dumps({"data": {}}))
        lat, lon = m.load_location(str(tmp_path))
        assert lat == pytest.approx(0.0)
        assert lon == pytest.approx(0.0)

    def test_negative_coordinates(self, m, tmp_path):
        storage = tmp_path / ".storage"
        storage.mkdir()
        config = {"data": {"latitude": -33.87, "longitude": 151.21}}
        (storage / "core.config").write_text(json.dumps(config))
        lat, lon = m.load_location(str(tmp_path))
        assert lat == pytest.approx(-33.87)
        assert lon == pytest.approx(151.21)


# ===========================================================================
# _get_bayesian_window_start
# ===========================================================================


class TestGetBayesianWindowStart:
    def test_returns_min_timestamp_across_entities(self, m, states_db):
        # sensor.foo first entry at t=1000, sensor.bar first at t=1500 → min=1000
        result = m.get_bayesian_window_start(["sensor.foo", "sensor.bar"], states_db)
        assert result == pytest.approx(1000.0)

    def test_single_entity_returns_its_earliest_timestamp(self, m, states_db):
        result = m.get_bayesian_window_start(["sensor.bar"], states_db)
        assert result == pytest.approx(1500.0)

    def test_missing_entity_returns_none(self, m, states_db):
        result = m.get_bayesian_window_start(["sensor.nonexistent"], states_db)
        assert result is None


# ===========================================================================
# Additional _compute_bayesian_probability — extreme prior edge cases
# ===========================================================================


class TestComputeBayesianProbabilityEdgeCases:
    def test_prior_zero_with_all_skipped_is_clamped_to_min(self, m):
        # p stays 0.0 → clamped to 0.0001
        obs = [_obs(0.9, 0.1)]
        result = m.compute_bayesian_probability(0.0, obs, [None])
        assert result == pytest.approx(0.0001)

    def test_prior_one_with_all_skipped_is_clamped_to_max(self, m):
        # p stays 1.0 → clamped to 0.9999
        obs = [_obs(0.9, 0.1)]
        result = m.compute_bayesian_probability(1.0, obs, [None])
        assert result == pytest.approx(0.9999)

    def test_prior_zero_with_active_obs_is_clamped(self, m):
        # num = 0 * 0.9 = 0, denom = 0*0.9 + 1*0.1 = 0.1 → p=0 → clamped to 0.0001
        obs = [_obs(0.9, 0.1)]
        result = m.compute_bayesian_probability(0.0, obs, [True])
        assert result == pytest.approx(0.0001)

    def test_prior_one_with_inactive_obs_is_clamped(self, m):
        # num = 1*(1-0.9) = 0.1, denom = 1*0.1 + 0*(1-0.1) = 0.1 → p=1.0 → clamped to 0.9999
        obs = [_obs(0.9, 0.1)]
        result = m.compute_bayesian_probability(1.0, obs, [False])
        assert result == pytest.approx(0.9999)


# ===========================================================================
# _evaluate_observation — numeric_state strict boundary conditions
# ===========================================================================


class TestNumericStateBoundaryConditions:
    def _tl(self, state_val):
        return {"sensor.temp": {"ts": [500.0], "state": [str(state_val)], "attrs": [None]}}

    def _ctx(self):
        return {"ts": 1000.0}

    def test_value_equal_to_above_threshold_is_inactive(self, m):
        """Comparison is strict (val > above), so value == above must return False."""
        obs = {"platform": "numeric_state", "entity_id": "sensor.temp", "above": 20.0}
        assert m.evaluate_observation(obs, self._tl(20.0), {}, 0, self._ctx()) is False

    def test_value_equal_to_below_threshold_is_inactive(self, m):
        """Comparison is strict (val < below), so value == below must return False."""
        obs = {"platform": "numeric_state", "entity_id": "sensor.temp", "below": 25.0}
        assert m.evaluate_observation(obs, self._tl(25.0), {}, 0, self._ctx()) is False

    def test_value_just_above_lower_bound_is_active(self, m):
        obs = {"platform": "numeric_state", "entity_id": "sensor.temp", "above": 20.0}
        assert m.evaluate_observation(obs, self._tl(20.001), {}, 0, self._ctx()) is True

    def test_value_just_below_upper_bound_is_active(self, m):
        obs = {"platform": "numeric_state", "entity_id": "sensor.temp", "below": 25.0}
        assert m.evaluate_observation(obs, self._tl(24.999), {}, 0, self._ctx()) is True

    def test_negative_value_in_range(self, m):
        obs = {"platform": "numeric_state", "entity_id": "sensor.temp",
               "above": -10.0, "below": 0.0}
        assert m.evaluate_observation(obs, self._tl(-5.0), {}, 0, self._ctx()) is True

    def test_negative_value_at_boundary(self, m):
        obs = {"platform": "numeric_state", "entity_id": "sensor.temp", "above": -10.0}
        assert m.evaluate_observation(obs, self._tl(-10.0), {}, 0, self._ctx()) is False


# ===========================================================================
# _eval_template — exception handling and truthy/falsy string parsing
# ===========================================================================


class TestEvalTemplateExceptions:
    def test_render_exception_returns_false(self, m):
        """A template raising an exception during render must return False, not propagate."""
        jinja2 = pytest.importorskip("jinja2")
        env = jinja2.Environment(undefined=jinja2.StrictUndefined)
        tpl = env.from_string("{{ undefined_variable }}")
        assert m.eval_template(tpl, {"ts": 1000.0}) is False

    def test_falsy_strings_return_false(self, m):
        jinja2 = pytest.importorskip("jinja2")
        env = jinja2.Environment()
        for falsy in ("false", "False", "FALSE", "0", "off", "no", "unknown", ""):
            tpl = env.from_string(falsy)
            result = m.eval_template(tpl, {"ts": 1000.0})
            assert result is False, f"Expected False for {falsy!r}"

    def test_truthy_strings_return_true(self, m):
        jinja2 = pytest.importorskip("jinja2")
        env = jinja2.Environment()
        for truthy in ("true", "True", "TRUE", "1", "yes", "YES", "True  "):
            tpl = env.from_string(truthy)
            result = m.eval_template(tpl, {"ts": 1000.0})
            assert result is True, f"Expected True for {truthy!r}"


# ===========================================================================
# _build_jinja2_env — state_attr() for non-sun entities
# ===========================================================================


class TestBuildJinja2EnvStateAttr:
    def test_state_attr_returns_value_for_known_entity(self, m):
        pytest.importorskip("jinja2")
        tl = {
            "sensor.foo": {
                "ts": [500.0],
                "state": ["on"],
                "attrs": ['{"brightness": 128}'],
            }
        }
        env, ctx = m.build_jinja2_env(tl, 51.5, -0.23)
        ctx["ts"] = 1000.0
        result = env.globals["state_attr"]("sensor.foo", "brightness")
        assert result == 128

    def test_state_attr_missing_entity_returns_none(self, m):
        pytest.importorskip("jinja2")
        env, ctx = m.build_jinja2_env({}, 51.5, -0.23)
        ctx["ts"] = 1000.0
        result = env.globals["state_attr"]("sensor.missing", "brightness")
        assert result is None

    def test_state_attr_missing_key_returns_none(self, m):
        pytest.importorskip("jinja2")
        tl = {
            "sensor.foo": {
                "ts": [500.0],
                "state": ["on"],
                "attrs": ['{"other_key": 42}'],
            }
        }
        env, ctx = m.build_jinja2_env(tl, 51.5, -0.23)
        ctx["ts"] = 1000.0
        result = env.globals["state_attr"]("sensor.foo", "brightness")
        assert result is None

    def test_state_attr_before_first_entry_returns_none(self, m):
        pytest.importorskip("jinja2")
        tl = {
            "sensor.foo": {
                "ts": [5000.0],
                "state": ["on"],
                "attrs": ['{"brightness": 50}'],
            }
        }
        env, ctx = m.build_jinja2_env(tl, 51.5, -0.23)
        ctx["ts"] = 1000.0  # before first entry at 5000
        result = env.globals["state_attr"]("sensor.foo", "brightness")
        assert result is None


# ===========================================================================
# _load_bayesian_config — YAML error handling
# ===========================================================================


class TestLoadBayesianConfigYAMLErrors:
    def test_malformed_yaml_file_is_skipped_and_valid_file_found(self, m, tmp_path):
        """A YAML file that contains 'platform: bayesian' but has invalid syntax is
        silently skipped; the search continues and finds the config in a valid file."""
        registry = {
            "data": {
                "entities": [
                    {
                        "entity_id": "binary_sensor.skip_sensor",
                        "platform": "bayesian",
                        "config_entry_id": None,
                        "unique_id": "bayesian-skip_sensor",
                    }
                ]
            }
        }
        storage = tmp_path / ".storage"
        storage.mkdir()
        (storage / "core.entity_registry").write_text(json.dumps(registry))

        # Contains "platform: bayesian" to pass the quick-scan, but has invalid YAML
        (tmp_path / "broken.yaml").write_text(
            "platform: bayesian\n  - bad_indent: [\n"
        )

        # Valid config file the function should fall through to
        (tmp_path / "valid_sensor.yaml").write_text("""
- platform: bayesian
  name: "Skip Sensor"
  unique_id: skip_sensor
  prior: 0.35
  probability_threshold: 0.6
  observations: []
""")

        cfg = m.load_bayesian_config("binary_sensor.skip_sensor", str(tmp_path))
        assert float(cfg["prior"]) == pytest.approx(0.35)

    def test_yaml_file_without_bayesian_platform_string_is_not_parsed(self, m, tmp_path):
        """Files that don't contain the string 'platform: bayesian' are skipped without
        a full YAML parse (two-pass optimisation)."""
        registry = {
            "data": {
                "entities": [
                    {
                        "entity_id": "binary_sensor.only_sensor",
                        "platform": "bayesian",
                        "config_entry_id": None,
                        "unique_id": "bayesian-only_sensor",
                    }
                ]
            }
        }
        storage = tmp_path / ".storage"
        storage.mkdir()
        (storage / "core.entity_registry").write_text(json.dumps(registry))

        # This file does NOT contain "platform: bayesian" → skipped by quick-scan
        (tmp_path / "irrelevant.yaml").write_text(
            "input_boolean:\n  some_helper:\n    name: Helper\n"
        )

        # The real config
        (tmp_path / "sensor.yaml").write_text("""
- platform: bayesian
  name: "Only Sensor"
  unique_id: only_sensor
  prior: 0.55
  probability_threshold: 0.7
  observations: []
""")

        cfg = m.load_bayesian_config("binary_sensor.only_sensor", str(tmp_path))
        assert float(cfg["prior"]) == pytest.approx(0.55)


# ===========================================================================
# _backfill_single_bayesian — integration tests
# ===========================================================================


def _make_async(fn):
    """Wrap a synchronous function so it can be awaited in an async context."""
    async def wrapper(*args, **kwargs):
        return fn(*args, **kwargs)
    return wrapper


_MOTION_ATTRS = json.dumps({
    "probability": 0.5,
    "probability_threshold": 0.7,
    "occurred_observation_entities": [],
    "observations": [],
    "friendly_name": "Test motion",
})


@pytest.fixture
def backfill_db(tmp_path):
    """SQLite DB with a single motion sensor for backfill integration tests.

    Includes full states schema (origin_idx, state_attributes, states_meta for
    binary_sensor.test) so write_history tests can write and verify results.
    """
    db_path = str(tmp_path / "ha.db")
    conn = sqlite3.connect(db_path)
    conn.executescript(f"""
        CREATE TABLE states_meta (
            metadata_id INTEGER PRIMARY KEY,
            entity_id   TEXT NOT NULL
        );
        CREATE TABLE state_attributes (
            attributes_id INTEGER PRIMARY KEY,
            hash          INTEGER,
            shared_attrs  TEXT
        );
        CREATE TABLE states (
            state_id        INTEGER PRIMARY KEY,
            metadata_id     INTEGER,
            state           TEXT,
            last_updated_ts REAL,
            last_changed_ts REAL,
            last_reported_ts REAL,
            old_state_id    INTEGER,
            attributes_id   INTEGER,
            context_id_bin  BLOB,
            origin_idx      INTEGER DEFAULT 0
        );
        INSERT INTO states_meta VALUES (1, 'binary_sensor.motion');
        INSERT INTO states_meta VALUES (2, 'binary_sensor.test');
        INSERT INTO state_attributes VALUES (1, 12345, '{_MOTION_ATTRS}');
        INSERT INTO states (state_id, metadata_id, state, last_updated_ts, attributes_id, origin_idx)
            VALUES (1, 1, 'on',  0.0,    NULL, 0);
        INSERT INTO states (state_id, metadata_id, state, last_updated_ts, attributes_id, origin_idx)
            VALUES (2, 1, 'off', 3600.0, NULL, 0);
        INSERT INTO states (state_id, metadata_id, state, last_updated_ts, attributes_id, origin_idx)
            VALUES (3, 1, 'on',  7200.0, NULL, 0);
        -- Real state for binary_sensor.test (needed as attributes template)
        INSERT INTO states (state_id, metadata_id, state, last_updated_ts, attributes_id, origin_idx)
            VALUES (4, 2, 'off', -1.0, 1, 0);
    """)
    conn.commit()
    conn.close()
    return db_path


@pytest.fixture
def async_module():
    """Load __init__.py with pyscript stubs, wrapping executor functions as async.

    The executor-decorated functions in __init__.py delegate to core.* which are
    plain synchronous functions. We wrap them as coroutines so they can be awaited.
    """
    init_mod = _load_init_module()
    executor_fns = ("_get_bayesian_window_start", "_load_state_timelines",
                    "_load_existing_sensor_states",
                    "_prepare_history_write", "_write_history_batch",
                    "_compute_backfill_rows")
    originals = {name: getattr(init_mod, name) for name in executor_fns}
    for name in executor_fns:
        setattr(init_mod, name, _make_async(originals[name]))
    yield init_mod
    for name, orig in originals.items():
        setattr(init_mod, name, orig)


class TestBackfillSingleBayesian:
    """Integration tests for _backfill_single_bayesian.

    The async_module fixture wraps the executor-decorated functions as coroutines.
    Tests run via asyncio.run(). The backfill_db must include states_meta,
    state_attributes, and full states schema for write_history to work.
    """

    def _motion_cfg(self, prior=0.3):
        return {
            "prior": prior,
            "probability_threshold": 0.7,
            "observations": [
                {
                    "platform": "state",
                    "entity_id": "binary_sensor.motion",
                    "to_state": "on",
                    "prob_given_true": 0.9,
                    "prob_given_false": 0.1,
                }
            ],
        }

    def _run(self, m, cfg, *, start, end, dry_run=False, db):
        count, result = asyncio.run(
            m._backfill_single_bayesian(
                "binary_sensor.test", cfg,
                user_start_ts=start, end_ts=end,
                dry_run=dry_run, debug=False,
                db_path=db, lat=51.5, lon=0.0,
            )
        )
        return count, result

    def _read_backfill_rows(self, db_path):
        """Read all backfill-written rows from the DB, ordered by timestamp."""
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT s.last_updated_ts, s.state, sa.shared_attrs FROM states s "
            "JOIN state_attributes sa ON s.attributes_id = sa.attributes_id "
            "WHERE s.origin_idx = 2 ORDER BY s.last_updated_ts"
        ).fetchall()
        conn.close()
        return rows

    def test_returns_correct_row_count(self, async_module, backfill_db):
        # motion has state changes at t=0, 3600, 7200 → 3 events in [0, 10800)
        count, result = self._run(async_module, self._motion_cfg(), start=0.0, end=10800.0, db=backfill_db)
        assert count == 3
        assert result["events"] == 3

    def test_dry_run_does_not_write_history(self, async_module, backfill_db):
        count, result = self._run(async_module, self._motion_cfg(), start=0.0, end=10800.0,
                                  dry_run=True, db=backfill_db)
        conn = sqlite3.connect(backfill_db)
        db_count = conn.execute(
            "SELECT COUNT(*) FROM states WHERE origin_idx = 2"
        ).fetchone()[0]
        conn.close()
        assert db_count == 0
        assert "sample" in result  # dry_run includes sample

    def test_computed_probabilities_match_bayes_formula(self, async_module, backfill_db):
        """Verify end-to-end probability computation for known motion states."""
        self._run(async_module, self._motion_cfg(prior=0.3), start=0.0, end=10800.0,
                  db=backfill_db)
        rows = self._read_backfill_rows(backfill_db)
        assert len(rows) == 3
        probs = [json.loads(r["shared_attrs"])["probability"] for r in rows]
        expected_on = 0.3 * 0.9 / (0.3 * 0.9 + 0.7 * 0.1)
        expected_off = 0.3 * 0.1 / (0.3 * 0.1 + 0.7 * 0.9)
        assert abs(probs[0] - expected_on) < 1e-4
        assert abs(probs[1] - expected_off) < 1e-4

    def test_all_probabilities_within_clamped_range(self, async_module, backfill_db):
        self._run(async_module, self._motion_cfg(), start=0.0, end=10800.0, db=backfill_db)
        rows = self._read_backfill_rows(backfill_db)
        for r in rows:
            prob = json.loads(r["shared_attrs"])["probability"]
            assert 0.0001 <= prob <= 0.9999

    def test_empty_window_returns_zero_rows(self, async_module, backfill_db):
        count, result = self._run(async_module, self._motion_cfg(), start=10800.0, end=0.0,
                                  db=backfill_db)
        assert count == 0

    def test_no_observations_uses_prior(self, async_module, backfill_db):
        """No observations → no timelines → single row at resolved_start with prior."""
        cfg = {"prior": 0.42, "probability_threshold": 0.5, "observations": []}
        count, result = self._run(async_module, cfg, start=0.0, end=7200.0, db=backfill_db)
        assert count == 1
        db_rows = self._read_backfill_rows(backfill_db)
        assert len(db_rows) == 1
        prob = json.loads(db_rows[0]["shared_attrs"])["probability"]
        assert abs(prob - 0.42) < 1e-4

    def test_occurred_observation_entities_reflects_active_observations(self, async_module, backfill_db):
        self._run(async_module, self._motion_cfg(), start=0.0, end=10800.0, db=backfill_db)
        rows = self._read_backfill_rows(backfill_db)
        attrs_0 = json.loads(rows[0]["shared_attrs"])
        assert "binary_sensor.motion" in attrs_0["occurred_observation_entities"]
        attrs_1 = json.loads(rows[1]["shared_attrs"])
        assert "binary_sensor.motion" not in attrs_1["occurred_observation_entities"]
        attrs_2 = json.loads(rows[2]["shared_attrs"])
        assert "binary_sensor.motion" in attrs_2["occurred_observation_entities"]

    def test_observations_attribute_has_observed_false_on_inactive(self, async_module, backfill_db):
        self._run(async_module, self._motion_cfg(), start=0.0, end=10800.0, db=backfill_db)
        rows = self._read_backfill_rows(backfill_db)
        obs_0 = json.loads(rows[0]["shared_attrs"])["observations"][0]
        assert "observed" not in obs_0
        obs_1 = json.loads(rows[1]["shared_attrs"])["observations"][0]
        assert obs_1["observed"] is False

    def test_result_contains_diff_and_timing(self, async_module, backfill_db):
        count, result = self._run(async_module, self._motion_cfg(), start=0.0, end=10800.0,
                                  db=backfill_db)
        assert "diff" in result
        assert result["diff"]["total_events"] == 3
        assert "computation_seconds" in result
        assert result["estimated_write_seconds"] >= 0

    def test_result_warnings_for_missing_entity(self, async_module, backfill_db):
        """Observation referencing an entity not in the DB should produce a warning."""
        cfg = {
            "prior": 0.5, "probability_threshold": 0.5,
            "observations": [{
                "platform": "state", "entity_id": "binary_sensor.nonexistent",
                "to_state": "on", "prob_given_true": 0.9, "prob_given_false": 0.1,
            }],
        }
        count, result = self._run(async_module, cfg, start=0.0, end=7200.0, db=backfill_db)
        warning_types = [w["type"] for w in result["warnings"]]
        assert "missing_entity_history" in warning_types


# ===========================================================================
# _read_purge_keep_days
# ===========================================================================


@pytest.fixture
def purge_config_dir(tmp_path):
    """config_dir with configuration.yaml declaring purge_keep_days."""
    (tmp_path / "configuration.yaml").write_text(
        "recorder:\n  purge_keep_days: 400\n  commit_interval: 1\n"
    )
    return tmp_path


@pytest.fixture
def purge_states_db(tmp_path):
    """Minimal states DB whose oldest record is ~50 days ago."""
    db_path = str(tmp_path / "test.db")
    import time as _time
    oldest_ts = _time.time() - 50 * 86400
    conn = sqlite3.connect(db_path)
    conn.executescript(f"""
        CREATE TABLE states (
            state_id INTEGER PRIMARY KEY,
            metadata_id INTEGER,
            state TEXT,
            last_updated_ts REAL,
            last_changed_ts REAL,
            last_reported_ts REAL,
            old_state_id INTEGER,
            attributes_id INTEGER,
            context_id_bin BLOB,
            origin_idx INTEGER
        );
        INSERT INTO states (state_id, metadata_id, state, last_updated_ts, origin_idx)
            VALUES (1, 1, 'on', {oldest_ts}, 0);
        INSERT INTO states (state_id, metadata_id, state, last_updated_ts, origin_idx)
            VALUES (2, 1, 'off', {oldest_ts + 86400}, 0);
    """)
    conn.commit()
    conn.close()
    return db_path


class TestReadPurgeKeepDays:
    def test_reads_from_configuration_yaml(self, m, purge_config_dir, purge_states_db):
        result = m.read_purge_keep_days(str(purge_config_dir), purge_states_db)
        assert result == 400

    def test_falls_back_to_db_oldest_state(self, m, tmp_path, purge_states_db):
        """No configuration.yaml → use oldest state row to infer retention."""
        # tmp_path has no configuration.yaml
        result = m.read_purge_keep_days(str(tmp_path), purge_states_db)
        # Oldest row is ~50 days ago; allow ±2 days for timing
        assert 48 <= result <= 52

    def test_falls_back_to_10_when_no_config_and_empty_db(self, m, tmp_path):
        """No config file and no DB → hardcoded default of 10."""
        missing_db = str(tmp_path / "nonexistent.db")
        result = m.read_purge_keep_days(str(tmp_path), missing_db)
        assert result == 10

    def test_falls_back_to_10_when_db_has_no_states(self, m, tmp_path):
        """Config absent; DB exists but states table is empty → default 10."""
        db_path = str(tmp_path / "empty.db")
        conn = sqlite3.connect(db_path)
        conn.execute(
            "CREATE TABLE states (state_id INTEGER PRIMARY KEY, last_updated_ts REAL)"
        )
        conn.commit()
        conn.close()
        result = m.read_purge_keep_days(str(tmp_path), db_path)
        assert result == 10


# ===========================================================================
# _write_bayesian_state_history
# ===========================================================================

_BAYESIAN_ATTRS = json.dumps({
    "probability": 0.5,
    "probability_threshold": 0.85,
    "occurred_observation_entities": [],
    "observations": [],
    "device_class": "occupancy",
    "friendly_name": "Test sensor",
})


@pytest.fixture
def bayesian_states_db(tmp_path):
    """Full-schema states DB with one Bayesian entity and one real state."""
    db_path = str(tmp_path / "bayesian.db")
    conn = sqlite3.connect(db_path)
    conn.executescript(f"""
        CREATE TABLE states_meta (
            metadata_id INTEGER PRIMARY KEY,
            entity_id   TEXT NOT NULL
        );
        CREATE TABLE state_attributes (
            attributes_id INTEGER PRIMARY KEY,
            hash          INTEGER,
            shared_attrs  TEXT
        );
        CREATE TABLE states (
            state_id        INTEGER PRIMARY KEY,
            metadata_id     INTEGER,
            state           TEXT,
            last_updated_ts REAL,
            last_changed_ts REAL,
            last_reported_ts REAL,
            old_state_id    INTEGER,
            attributes_id   INTEGER,
            context_id_bin  BLOB,
            origin_idx      INTEGER DEFAULT 0
        );

        INSERT INTO states_meta VALUES (1, 'binary_sensor.test_bayesian');

        INSERT INTO state_attributes VALUES (1, 12345, '{_BAYESIAN_ATTRS}');

        -- One real (origin_idx=0) state before our window
        INSERT INTO states (state_id, metadata_id, state, last_updated_ts,
                            attributes_id, origin_idx)
            VALUES (1, 1, 'off', 500.0, 1, 0);
    """)
    conn.commit()
    conn.close()
    return db_path


def _backfill_rows(start_ts, n, interval=3600.0, threshold=0.85):
    """Generate n synthetic rows alternating low/high probability."""
    rows = []
    for i in range(n):
        prob = 0.95 if i % 2 == 0 else 0.05
        rows.append({
            "ts": start_ts + i * interval,
            "probability": prob,
            "state": "on" if prob >= threshold else "off",
            "occurred_observation_entities": ["binary_sensor.motion"] if prob >= threshold else [],
            "observations": [{"platform": "state", "entity_id": "binary_sensor.motion",
                              "to_state": "on", "prob_given_true": 0.9, "prob_given_false": 0.1}],
        })
    return rows


class TestWriteHistoryBatch:
    """Tests for _prepare_history_write + _write_history_batch."""

    def _write_all(self, m, entity_id, rows, db_path):
        """Helper: prepare + write all rows in batches, like the real caller."""
        if not rows:
            return 0
        metadata_id, base_attrs, old_state_id = m.prepare_history_write(
            entity_id, db_path, rows[0]["ts"]
        )
        total = 0
        prev_state_val = None
        batch_size = m._WRITE_BATCH_SIZE
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            inserted, old_state_id, prev_state_val = m.write_history_batch(
                batch, metadata_id, base_attrs, old_state_id, prev_state_val, db_path
            )
            total += inserted
        return total

    def test_returns_inserted_count(self, m, bayesian_states_db):
        rows = _backfill_rows(1000.0, 5)
        inserted = self._write_all(m, "binary_sensor.test_bayesian", rows, bayesian_states_db)
        assert inserted == 5

    def test_rows_written_to_db(self, m, bayesian_states_db):
        rows = _backfill_rows(1000.0, 3)
        self._write_all(m, "binary_sensor.test_bayesian", rows, bayesian_states_db)
        conn = sqlite3.connect(bayesian_states_db)
        count = conn.execute(
            "SELECT COUNT(*) FROM states WHERE metadata_id = 1 AND origin_idx = 2"
        ).fetchone()[0]
        conn.close()
        assert count == 3

    def test_probability_attribute_written(self, m, bayesian_states_db):
        rows = [{"ts": 1000.0, "probability": 0.73, "state": "off",
                 "occurred_observation_entities": [], "observations": []}]
        self._write_all(m, "binary_sensor.test_bayesian", rows, bayesian_states_db)
        conn = sqlite3.connect(bayesian_states_db)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT sa.shared_attrs FROM states s "
            "JOIN state_attributes sa ON s.attributes_id = sa.attributes_id "
            "WHERE s.metadata_id = 1 AND s.origin_idx = 2"
        ).fetchone()
        conn.close()
        assert row is not None
        attrs = json.loads(row["shared_attrs"])
        assert abs(attrs["probability"] - 0.73) < 1e-6

    def test_occurred_and_observations_written(self, m, bayesian_states_db):
        obs_list = [{"platform": "state", "entity_id": "binary_sensor.motion",
                     "to_state": "on", "prob_given_true": 0.9, "prob_given_false": 0.1}]
        rows = [{"ts": 1000.0, "probability": 0.9, "state": "on",
                 "occurred_observation_entities": ["binary_sensor.motion"],
                 "observations": obs_list}]
        self._write_all(m, "binary_sensor.test_bayesian", rows, bayesian_states_db)
        conn = sqlite3.connect(bayesian_states_db)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT sa.shared_attrs FROM states s "
            "JOIN state_attributes sa ON s.attributes_id = sa.attributes_id "
            "WHERE s.metadata_id = 1 AND s.origin_idx = 2"
        ).fetchone()
        conn.close()
        attrs = json.loads(row["shared_attrs"])
        assert attrs["occurred_observation_entities"] == ["binary_sensor.motion"]
        assert attrs["observations"] == obs_list

    def test_uses_backfill_origin_idx(self, m, bayesian_states_db):
        rows = _backfill_rows(1000.0, 4)
        self._write_all(m, "binary_sensor.test_bayesian", rows, bayesian_states_db)
        conn = sqlite3.connect(bayesian_states_db)
        origin_values = [
            r[0] for r in conn.execute(
                "SELECT origin_idx FROM states WHERE metadata_id = 1 AND last_updated_ts >= 1000"
            ).fetchall()
        ]
        conn.close()
        assert all(v == 2 for v in origin_values)

    def test_idempotent_reruns(self, m, bayesian_states_db):
        rows = _backfill_rows(1000.0, 5)
        self._write_all(m, "binary_sensor.test_bayesian", rows, bayesian_states_db)
        self._write_all(m, "binary_sensor.test_bayesian", rows, bayesian_states_db)
        conn = sqlite3.connect(bayesian_states_db)
        count = conn.execute(
            "SELECT COUNT(*) FROM states WHERE metadata_id = 1 AND origin_idx = 2"
        ).fetchone()[0]
        conn.close()
        assert count == 5  # not 10

    def test_does_not_delete_real_states(self, m, bayesian_states_db):
        rows = _backfill_rows(100.0, 3)  # window overlaps the real state at t=500
        self._write_all(m, "binary_sensor.test_bayesian", rows, bayesian_states_db)
        conn = sqlite3.connect(bayesian_states_db)
        real_count = conn.execute(
            "SELECT COUNT(*) FROM states WHERE metadata_id = 1 AND origin_idx = 0"
        ).fetchone()[0]
        conn.close()
        assert real_count == 1  # original real state preserved

    def test_last_changed_ts_set_on_state_transition(self, m, bayesian_states_db):
        rows = [
            {"ts": 1000.0, "probability": 0.95, "state": "on",
             "occurred_observation_entities": [], "observations": []},
            {"ts": 2000.0, "probability": 0.90, "state": "on",
             "occurred_observation_entities": [], "observations": []},
            {"ts": 3000.0, "probability": 0.05, "state": "off",
             "occurred_observation_entities": [], "observations": []},
        ]
        self._write_all(m, "binary_sensor.test_bayesian", rows, bayesian_states_db)
        conn = sqlite3.connect(bayesian_states_db)
        results = conn.execute(
            "SELECT last_updated_ts, last_changed_ts FROM states "
            "WHERE metadata_id = 1 AND origin_idx = 2 ORDER BY last_updated_ts"
        ).fetchall()
        conn.close()
        assert results[0][1] == 1000.0   # first row: changed
        assert results[1][1] is None     # same state: not changed
        assert results[2][1] == 3000.0   # transition: changed

    def test_empty_rows_returns_zero(self, m, bayesian_states_db):
        result = self._write_all(m, "binary_sensor.test_bayesian", [], bayesian_states_db)
        assert result == 0

    def test_missing_entity_raises(self, m, bayesian_states_db):
        rows = _backfill_rows(1000.0, 2)
        with pytest.raises(ValueError, match="No states_meta entry"):
            self._write_all(m, "binary_sensor.nonexistent", rows, bayesian_states_db)


# ===========================================================================
# compute_backfill_rows
# ===========================================================================


def _state_obs(entity_id, to_state, p_true=0.9, p_false=0.1):
    return {
        "platform": "state",
        "entity_id": entity_id,
        "to_state": to_state,
        "prob_given_true": p_true,
        "prob_given_false": p_false,
    }


def _tl(ts_list, state_list):
    return {"ts": list(ts_list), "state": list(state_list), "attrs": [None] * len(ts_list)}


class TestComputeBackfillRows:
    def test_returns_expected_keys(self, m):
        observations = [_state_obs("binary_sensor.a", "on")]
        timelines = {"binary_sensor.a": _tl([1000.0, 2000.0], ["off", "on"])}
        result = m.compute_backfill_rows(
            observations, prior=0.5, probability_threshold=0.5,
            timelines=timelines, resolved_start=0.0, end_ts=3000.0,
            lat=51.5, lon=0.0, debug=False,
        )
        for key in ("history_rows", "event_timestamps", "warnings",
                    "computation_seconds", "debug_messages", "template_errors"):
            assert key in result, f"Missing key: {key}"

    def test_event_timestamps_from_timelines(self, m):
        observations = [_state_obs("binary_sensor.a", "on")]
        timelines = {"binary_sensor.a": _tl([1000.0, 2000.0, 2500.0], ["off", "on", "off"])}
        result = m.compute_backfill_rows(
            observations, 0.5, 0.5, timelines,
            resolved_start=500.0, end_ts=3000.0,
            lat=0.0, lon=0.0, debug=False,
        )
        # Only timestamps within [resolved_start, end_ts) are included
        assert result["event_timestamps"] == [1000.0, 2000.0, 2500.0]

    def test_event_timestamps_excludes_outside_window(self, m):
        observations = [_state_obs("binary_sensor.a", "on")]
        timelines = {"binary_sensor.a": _tl([100.0, 1500.0, 5000.0], ["off", "on", "off"])}
        result = m.compute_backfill_rows(
            observations, 0.5, 0.5, timelines,
            resolved_start=500.0, end_ts=3000.0,
            lat=0.0, lon=0.0, debug=False,
        )
        assert result["event_timestamps"] == [1500.0]

    def test_fallback_single_row_when_no_events(self, m):
        observations = [_state_obs("binary_sensor.a", "on")]
        # Timeline has no entries in window
        timelines = {"binary_sensor.a": _tl([], [])}
        result = m.compute_backfill_rows(
            observations, 0.5, 0.5, timelines,
            resolved_start=1000.0, end_ts=2000.0,
            lat=0.0, lon=0.0, debug=False,
        )
        assert result["event_timestamps"] == [1000.0]
        assert len(result["history_rows"]) == 1

    def test_state_row_reflects_probability(self, m):
        observations = [_state_obs("binary_sensor.a", "on", p_true=0.9, p_false=0.1)]
        timelines = {"binary_sensor.a": _tl([1000.0], ["on"])}
        result = m.compute_backfill_rows(
            observations, 0.5, 0.5, timelines,
            resolved_start=500.0, end_ts=2000.0,
            lat=0.0, lon=0.0, debug=False,
        )
        row = result["history_rows"][0]
        assert abs(row["probability"] - 0.9) < 1e-4
        assert row["state"] == "on"

    def test_state_off_below_threshold(self, m):
        # Entity "off" with p_true=0.9, p_false=0.1: inactive obs drives p to 0.1 → "off"
        observations = [_state_obs("binary_sensor.a", "on", p_true=0.9, p_false=0.1)]
        timelines = {"binary_sensor.a": _tl([1000.0], ["off"])}
        result = m.compute_backfill_rows(
            observations, 0.5, 0.5, timelines,
            resolved_start=500.0, end_ts=2000.0,
            lat=0.0, lon=0.0, debug=False,
        )
        row = result["history_rows"][0]
        assert row["state"] == "off"

    def test_occurred_entities_populated(self, m):
        observations = [_state_obs("binary_sensor.a", "on")]
        timelines = {"binary_sensor.a": _tl([1000.0], ["on"])}
        result = m.compute_backfill_rows(
            observations, 0.5, 0.5, timelines,
            resolved_start=500.0, end_ts=2000.0,
            lat=0.0, lon=0.0, debug=False,
        )
        assert "binary_sensor.a" in result["history_rows"][0]["occurred_observation_entities"]

    def test_inactive_obs_gets_observed_false(self, m):
        observations = [_state_obs("binary_sensor.a", "on")]
        timelines = {"binary_sensor.a": _tl([1000.0], ["off"])}
        result = m.compute_backfill_rows(
            observations, 0.5, 0.5, timelines,
            resolved_start=500.0, end_ts=2000.0,
            lat=0.0, lon=0.0, debug=False,
        )
        obs_attrs = result["history_rows"][0]["observations"][0]
        assert obs_attrs.get("observed") is False

    def test_warning_missing_entity_history(self, m):
        observations = [_state_obs("binary_sensor.missing", "on")]
        timelines = {}  # no data for the entity
        result = m.compute_backfill_rows(
            observations, 0.5, 0.5, timelines,
            resolved_start=0.0, end_ts=2000.0,
            lat=0.0, lon=0.0, debug=False,
        )
        types = [w["type"] for w in result["warnings"]]
        assert "missing_entity_history" in types

    def test_warning_always_active(self, m):
        observations = [_state_obs("binary_sensor.a", "on")]
        # Entity is always "on" → always active warning
        timelines = {"binary_sensor.a": _tl([1000.0, 2000.0], ["on", "on"])}
        result = m.compute_backfill_rows(
            observations, 0.5, 0.5, timelines,
            resolved_start=500.0, end_ts=3000.0,
            lat=0.0, lon=0.0, debug=False,
        )
        types = [w["type"] for w in result["warnings"]]
        assert "always_active" in types

    def test_warning_never_active(self, m):
        observations = [_state_obs("binary_sensor.a", "on")]
        # Entity is always "off" → never active warning
        timelines = {"binary_sensor.a": _tl([1000.0, 2000.0], ["off", "off"])}
        result = m.compute_backfill_rows(
            observations, 0.5, 0.5, timelines,
            resolved_start=500.0, end_ts=3000.0,
            lat=0.0, lon=0.0, debug=False,
        )
        types = [w["type"] for w in result["warnings"]]
        assert "never_active" in types

    def test_debug_messages_populated_when_debug_true(self, m):
        observations = [_state_obs("binary_sensor.a", "on")]
        timelines = {"binary_sensor.a": _tl([1000.0, 2000.0], ["off", "on"])}
        result = m.compute_backfill_rows(
            observations, 0.5, 0.5, timelines,
            resolved_start=500.0, end_ts=3000.0,
            lat=0.0, lon=0.0, debug=True,
        )
        assert len(result["debug_messages"]) > 0

    def test_debug_messages_empty_when_debug_false(self, m):
        observations = [_state_obs("binary_sensor.a", "on")]
        timelines = {"binary_sensor.a": _tl([1000.0], ["on"])}
        result = m.compute_backfill_rows(
            observations, 0.5, 0.5, timelines,
            resolved_start=500.0, end_ts=2000.0,
            lat=0.0, lon=0.0, debug=False,
        )
        assert result["debug_messages"] == []

    def test_debug_messages_capped_at_10(self, m):
        observations = [_state_obs("binary_sensor.a", "on")]
        # 20 events — debug should only capture the first 10
        ts_list = [float(1000 + i * 100) for i in range(20)]
        state_list = ["on" if i % 2 == 0 else "off" for i in range(20)]
        timelines = {"binary_sensor.a": _tl(ts_list, state_list)}
        result = m.compute_backfill_rows(
            observations, 0.5, 0.5, timelines,
            resolved_start=500.0, end_ts=5000.0,
            lat=0.0, lon=0.0, debug=True,
        )
        assert len(result["debug_messages"]) == 10

    def test_template_error_captured(self, m):
        observations = [{
            "platform": "template",
            "value_template": "{% this is invalid jinja %}",
            "prob_given_true": 0.8,
            "prob_given_false": 0.2,
        }]
        result = m.compute_backfill_rows(
            observations, 0.5, 0.5, {},
            resolved_start=0.0, end_ts=2000.0,
            lat=0.0, lon=0.0, debug=False,
        )
        assert len(result["template_errors"]) == 1
        assert result["template_errors"][0][0] == 0  # obs index

    def test_computation_seconds_is_float(self, m):
        observations = [_state_obs("binary_sensor.a", "on")]
        timelines = {"binary_sensor.a": _tl([1000.0], ["on"])}
        result = m.compute_backfill_rows(
            observations, 0.5, 0.5, timelines,
            resolved_start=500.0, end_ts=2000.0,
            lat=0.0, lon=0.0, debug=False,
        )
        assert isinstance(result["computation_seconds"], float)
        assert result["computation_seconds"] >= 0
