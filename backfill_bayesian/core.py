"""Pure-Python Bayesian backfill logic — no pyscript dependencies."""

__version__ = "0.1.0"

import bisect
import fnmatch
import json
import math
import os
import re
import sqlite3
import time
from datetime import datetime, timezone

_BACKFILL_ORIGIN_IDX = 2  # synthetic rows written by this service; never used by HA recorder

_WRITE_BATCH_SIZE = 500  # rows per DB commit to avoid blocking HA recorder


def solar_elevation(ts, lat_deg, lon_deg):
    """Compute solar elevation angle in degrees at a given Unix timestamp and location.

    Uses the Spencer equation for declination and an equation-of-time correction.
    Accurate to ~1 deg -- sufficient for the >5 deg threshold used in Bayesian templates.
    """
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    n = dt.timetuple().tm_yday
    B = math.radians(360 / 365 * (n - 81))
    decl = math.radians(23.45 * math.sin(B))
    eot = 9.87 * math.sin(2 * B) - 7.53 * math.cos(B) - 1.5 * math.sin(B)
    solar_time = dt.hour + dt.minute / 60 + dt.second / 3600 + lon_deg / 15 + eot / 60
    hour_angle = math.radians(15 * (solar_time - 12))
    lat = math.radians(lat_deg)
    return math.degrees(
        math.asin(
            math.sin(lat) * math.sin(decl)
            + math.cos(lat) * math.cos(decl) * math.cos(hour_angle)
        )
    )


def load_location(config_dir):
    """Load latitude/longitude from .storage/core.config."""
    path = os.path.join(config_dir, ".storage", "core.config")
    with open(path) as f:
        data = json.load(f)
    d = data.get("data", {})
    return [float(d.get("latitude", 0.0)), float(d.get("longitude", 0.0))]


def get_bayesian_entity_ids(pattern, config_dir):
    """Expand a glob pattern against all Bayesian sensor entity_ids in the registry.

    Returns a list of matching entity_ids (platform == 'bayesian').
    For a non-glob pattern, returns [pattern] only if it is a known Bayesian entity.
    """
    registry_path = os.path.join(config_dir, ".storage", "core.entity_registry")
    with open(registry_path) as f:
        registry = json.load(f)
    bayesian_ids = [
        e["entity_id"]
        for e in registry["data"]["entities"]
        if e.get("platform") == "bayesian"
    ]
    if "*" in pattern or "?" in pattern or "[" in pattern:
        return sorted(eid for eid in bayesian_ids if fnmatch.fnmatch(eid, pattern))
    return [pattern] if pattern in bayesian_ids else []


def load_bayesian_config(target_entity_id, config_dir):
    """Load Bayesian sensor config from UI config_entries or YAML files.

    For UI-created sensors: reads .storage/core.config_entries.
    For YAML-created sensors: recursively scans YAML files for platform: bayesian.

    Returns dict with keys: prior, probability_threshold, observations.
    Raises ValueError with diagnostic message if config not found.
    """
    import yaml

    # Look up entity in registry
    registry_path = os.path.join(config_dir, ".storage", "core.entity_registry")
    with open(registry_path) as f:
        registry = json.load(f)
    entry = next(
        (e for e in registry["data"]["entities"] if e["entity_id"] == target_entity_id),
        None,
    )
    if entry is None:
        raise ValueError(f"Entity {target_entity_id!r} not found in entity registry")

    config_entry_id = entry.get("config_entry_id")
    unique_id = entry.get("unique_id", "")

    # UI-defined: load from core.config_entries
    if config_entry_id:
        ce_path = os.path.join(config_dir, ".storage", "core.config_entries")
        with open(ce_path) as f:
            ce = json.load(f)
        cfg = next(
            (e for e in ce["data"]["entries"] if e["entry_id"] == config_entry_id),
            None,
        )
        if cfg is None:
            raise ValueError(
                f"Config entry {config_entry_id!r} not found for {target_entity_id!r}"
            )
        return cfg["data"]

    # YAML-defined: strip "bayesian-" prefix and search YAML files
    yaml_unique_id = unique_id.removeprefix("bayesian-")
    scanned = []

    skip_dirs = {".", "venv", "node_modules", "__pycache__", "deps", ".storage"}
    for root, dirs, files in os.walk(config_dir):
        dirs[:] = [d for d in dirs if d not in skip_dirs and not d.startswith(".")]
        for fname in files:
            if not (fname.endswith(".yaml") or fname.endswith(".yml")):
                continue
            fpath = os.path.join(root, fname)
            try:
                with open(fpath) as f:
                    raw = f.read()
            except OSError:
                continue
            # Two-pass: quick string check before full parse
            if "platform: bayesian" not in raw:
                continue
            scanned.append(fpath)
            try:
                data = yaml.safe_load(raw)
            except yaml.YAMLError:
                continue
            if data is None:
                continue
            # Handle bare list or nested {binary_sensor: [...], ...} package format
            candidates = []
            if isinstance(data, list):
                candidates = data
            elif isinstance(data, dict):
                for val in data.values():
                    if isinstance(val, list):
                        candidates.extend(val)
            for sensor in candidates:
                if not isinstance(sensor, dict):
                    continue
                if sensor.get("platform") != "bayesian":
                    continue
                if sensor.get("unique_id") == yaml_unique_id:
                    return sensor

    raise ValueError(
        f"Bayesian config for {target_entity_id!r} (unique_id={yaml_unique_id!r}) "
        f"not found in {len(scanned)} YAML file(s). Scanned: {scanned}"
    )


def read_purge_keep_days(config_dir, db_path):
    """Return recorder retention in days.

    Resolution order:
    1. purge_keep_days from configuration.yaml
    2. Oldest last_updated_ts in the states table (actual observed retention)
    3. 10 days (HA default fallback)
    """
    import re as _re

    # 1. configuration.yaml
    config_file = os.path.join(config_dir, "configuration.yaml")
    try:
        with open(config_file) as f:
            content = f.read()
        m = _re.search(r"^\s*purge_keep_days\s*:\s*(\d+)", content, _re.MULTILINE)
        if m:
            return int(m.group(1))
    except OSError:
        pass

    # 2. Oldest state row in the database
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=30)
        try:
            row = conn.execute("SELECT MIN(last_updated_ts) FROM states").fetchone()
            if row and row[0]:
                days = int((time.time() - row[0]) / 86400)
                if days > 0:
                    return days
        finally:
            conn.close()
    except Exception:
        pass

    # 3. HA default
    return 10


def load_existing_sensor_states(entity_id, start_ts, end_ts, db_path):
    """Load existing state records for a Bayesian sensor from the DB.

    Returns a sorted list of (ts, probability, state) tuples for comparison.
    """
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        meta = conn.execute(
            "SELECT metadata_id FROM states_meta WHERE entity_id = ?", (entity_id,)
        ).fetchone()
        if not meta:
            return []
        rows = conn.execute(
            "SELECT s.state, s.last_updated_ts, sa.shared_attrs "
            "FROM states s "
            "LEFT JOIN state_attributes sa ON s.attributes_id = sa.attributes_id "
            "WHERE s.metadata_id = ? AND s.last_updated_ts >= ? AND s.last_updated_ts <= ? "
            "ORDER BY s.last_updated_ts",
            (meta["metadata_id"], start_ts, end_ts),
        ).fetchall()
        result = []
        for r in rows:
            prob = None
            if r["shared_attrs"]:
                try:
                    prob = json.loads(r["shared_attrs"]).get("probability")
                except (json.JSONDecodeError, TypeError):
                    pass
            result.append((r["last_updated_ts"], prob, r["state"]))
        return result
    finally:
        conn.close()


def prepare_history_write(entity_id, db_path, min_ts):
    """Look up metadata and base attributes for state history writes.

    Returns (metadata_id, base_attrs, old_state_id) or raises ValueError.
    Opens and closes its own connection -- does not hold a lock.
    """
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        meta = conn.execute(
            "SELECT metadata_id FROM states_meta WHERE entity_id = ?", (entity_id,)
        ).fetchone()
        if not meta:
            raise ValueError(f"No states_meta entry for {entity_id}")
        metadata_id = meta["metadata_id"]

        recent = conn.execute(
            "SELECT sa.shared_attrs FROM states s "
            "LEFT JOIN state_attributes sa ON s.attributes_id = sa.attributes_id "
            "WHERE s.metadata_id = ? AND s.origin_idx != ? "
            "ORDER BY s.last_updated_ts DESC LIMIT 1",
            (metadata_id, _BACKFILL_ORIGIN_IDX),
        ).fetchone()
        if not recent or not recent["shared_attrs"]:
            raise ValueError(f"No existing real state for {entity_id} to use as attributes template")
        base_attrs = json.loads(recent["shared_attrs"])

        prev = conn.execute(
            "SELECT state_id FROM states WHERE metadata_id = ? AND last_updated_ts < ? "
            "ORDER BY last_updated_ts DESC LIMIT 1",
            (metadata_id, min_ts),
        ).fetchone()
        old_state_id = prev["state_id"] if prev else None

        return [metadata_id, base_attrs, old_state_id]
    finally:
        conn.close()


def write_history_batch(batch, metadata_id, base_attrs, old_state_id, prev_state_val, db_path):
    """Write one batch of state history rows. Opens/closes its own DB connection.

    Returns (inserted_count, last_old_state_id, last_prev_state_val).
    """
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        batch_min_ts = batch[0]["ts"]
        batch_max_ts = batch[-1]["ts"]

        conn.execute(
            "DELETE FROM states WHERE metadata_id = ? AND origin_idx = ? "
            "AND last_updated_ts >= ? AND last_updated_ts <= ?",
            (metadata_id, _BACKFILL_ORIGIN_IDX, batch_min_ts, batch_max_ts + 1),
        )

        inserted = 0
        for row in batch:
            ts = row["ts"]
            prob = row["probability"]
            state = row["state"]

            attrs = dict(base_attrs)
            attrs["probability"] = round(prob, 6)
            attrs["occurred_observation_entities"] = row.get(
                "occurred_observation_entities", []
            )
            attrs["observations"] = row.get("observations", [])
            attrs_json = json.dumps(attrs, separators=(",", ":"), sort_keys=False)
            attrs_hash = hash(attrs_json) & 0x7FFFFFFFFFFFFFFF

            existing_attr = conn.execute(
                "SELECT attributes_id FROM state_attributes WHERE hash = ? AND shared_attrs = ?",
                (attrs_hash, attrs_json),
            ).fetchone()
            if existing_attr:
                attributes_id = existing_attr["attributes_id"]
            else:
                conn.execute(
                    "INSERT INTO state_attributes (hash, shared_attrs) VALUES (?, ?)",
                    (attrs_hash, attrs_json),
                )
                attributes_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

            last_changed_ts = ts if state != prev_state_val else None

            conn.execute(
                """INSERT INTO states (
                    state, last_updated_ts, last_changed_ts, last_reported_ts,
                    old_state_id, attributes_id, context_id_bin, origin_idx, metadata_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    state, ts, last_changed_ts, ts,
                    old_state_id, attributes_id, os.urandom(16),
                    _BACKFILL_ORIGIN_IDX, metadata_id,
                ),
            )
            old_state_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            prev_state_val = state
            inserted += 1

        conn.commit()
        return [inserted, old_state_id, prev_state_val]
    finally:
        conn.close()


def get_bayesian_window_start(entity_ids, db_path):
    """Return MIN(last_updated_ts) across all given entity_ids in the states table."""
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=30)
    try:
        placeholders = ",".join("?" * len(entity_ids))
        meta_rows = conn.execute(
            f"SELECT metadata_id FROM states_meta WHERE entity_id IN ({placeholders})",
            entity_ids,
        ).fetchall()
        if not meta_rows:
            return None
        meta_ids = [r[0] for r in meta_rows]
        ph2 = ",".join("?" * len(meta_ids))
        row = conn.execute(
            f"SELECT MIN(last_updated_ts) FROM states WHERE metadata_id IN ({ph2})",
            meta_ids,
        ).fetchone()
        return row[0] if row and row[0] is not None else None
    finally:
        conn.close()


def load_state_timelines(entity_ids, start_ts, end_ts, load_attrs, db_path):
    """Bulk-load state histories for all entity_ids in the window.

    Queries from (start_ts - 86400) for a 1-day lookback seed for forward-fill.
    Returns {entity_id: {"ts": [...], "state": [...], "attrs": [...]}}, sorted ascending.
    "attrs" entries are raw JSON strings when load_attrs=True, else None.

    Two-step: resolve entity_ids -> metadata_ids first so the main query can use
    HA's (metadata_id, last_updated_ts) composite index instead of joining across
    the full states table.
    """
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        placeholders = ",".join("?" * len(entity_ids))
        lookback = start_ts - 86400

        # Step 1: metadata_id -> entity_id mapping (indexed lookup)
        meta_rows = conn.execute(
            f"SELECT entity_id, metadata_id FROM states_meta WHERE entity_id IN ({placeholders})",
            entity_ids,
        ).fetchall()
        if not meta_rows:
            return {}
        meta_id_to_entity = {r["metadata_id"]: r["entity_id"] for r in meta_rows}
        meta_ids = list(meta_id_to_entity.keys())
        ph2 = ",".join("?" * len(meta_ids))

        # Step 2: query states via (metadata_id, last_updated_ts) composite index
        if load_attrs:
            rows = conn.execute(
                f"""
                SELECT s.metadata_id, s.state, s.last_updated_ts, sa.shared_attrs
                FROM states s
                LEFT JOIN state_attributes sa ON sa.attributes_id = s.attributes_id
                WHERE s.metadata_id IN ({ph2})
                  AND s.last_updated_ts >= ?
                  AND s.last_updated_ts <= ?
                ORDER BY s.metadata_id, s.last_updated_ts
                """,
                [*meta_ids, lookback, end_ts],
            ).fetchall()
        else:
            rows = conn.execute(
                f"""
                SELECT s.metadata_id, s.state, s.last_updated_ts
                FROM states s
                WHERE s.metadata_id IN ({ph2})
                  AND s.last_updated_ts >= ?
                  AND s.last_updated_ts <= ?
                ORDER BY s.metadata_id, s.last_updated_ts
                """,
                [*meta_ids, lookback, end_ts],
            ).fetchall()

        result = {}
        for row in rows:
            eid = meta_id_to_entity[row["metadata_id"]]
            if eid not in result:
                result[eid] = {"ts": [], "state": [], "attrs": []}
            result[eid]["ts"].append(row["last_updated_ts"])
            result[eid]["state"].append(row["state"])
            result[eid]["attrs"].append(row["shared_attrs"] if load_attrs else None)
        return result
    finally:
        conn.close()


def get_state_at(timeline, ts):
    """Forward-fill: return the last known state at or before ts, or None."""
    if not timeline or not timeline["ts"]:
        return None
    idx = bisect.bisect_right(timeline["ts"], ts) - 1
    return timeline["state"][idx] if idx >= 0 else None


def get_attr_at(timeline, ts, attr_name):
    """Forward-fill: return the value of attr_name at or before ts, or None."""
    if not timeline or not timeline["ts"]:
        return None
    idx = bisect.bisect_right(timeline["ts"], ts) - 1
    if idx < 0:
        return None
    attrs_json = timeline["attrs"][idx]
    if not attrs_json:
        return None
    try:
        return json.loads(attrs_json).get(attr_name)
    except (json.JSONDecodeError, TypeError):
        return None


def build_jinja2_env(timelines, lat, lon):
    """Build a Jinja2 environment with historical states()/state_attr()/now() mocks.

    Returns (env, ctx). Set ctx["ts"] to the target Unix timestamp before each evaluation.
    state_attr("sun.sun", "elevation") is computed astronomically from lat/lon.
    """
    from jinja2 import Environment, Undefined

    ctx = {"ts": 0.0}

    def _states(entity_id):
        tl = timelines.get(entity_id)
        if tl is None:
            return "unknown"
        state = get_state_at(tl, ctx["ts"])
        return state if state is not None else "unknown"

    def _state_attr(entity_id, attribute):
        if entity_id == "sun.sun" and attribute == "elevation":
            return solar_elevation(ctx["ts"], lat, lon)
        tl = timelines.get(entity_id)
        if tl is None:
            return None
        return get_attr_at(tl, ctx["ts"], attribute)

    def _now():
        return datetime.fromtimestamp(ctx["ts"], tz=timezone.utc)

    env = Environment(undefined=Undefined)
    env.globals.update({"states": _states, "state_attr": _state_attr, "now": _now})
    return env, ctx


def eval_template(template_obj, ctx):
    """Render a pre-compiled Jinja2 template and return a bool. Returns False on error."""
    try:
        result = template_obj.render().strip().lower()
        return result in ("true", "1", "yes")
    except Exception:
        return False


def evaluate_observation(obs, timelines, compiled_templates, idx, ctx):
    """Evaluate one observation at ctx["ts"].

    Returns True if active, False if inactive, or None to skip
    (entity has no history at this timestamp -- excluded from Bayes update).
    """
    platform = obs.get("platform")

    if platform == "state":
        eid = obs.get("entity_id")
        tl = timelines.get(eid)
        state = get_state_at(tl, ctx["ts"]) if tl else None
        if state is None:
            return None
        return state == obs.get("to_state")

    if platform == "numeric_state":
        eid = obs.get("entity_id")
        tl = timelines.get(eid)
        raw = get_state_at(tl, ctx["ts"]) if tl else None
        if raw is None:
            return None
        try:
            val = float(raw)
        except (ValueError, TypeError):
            return None
        above = float(obs["above"]) if "above" in obs else float("-inf")
        below = float(obs["below"]) if "below" in obs else float("inf")
        return val > above and val < below

    if platform == "template":
        tpl = compiled_templates.get(idx)
        if tpl is None:
            return False
        return eval_template(tpl, ctx)

    return None


def compute_bayesian_probability(prior, observations, obs_results):
    """Iterative Bayes update over all observations.

    obs_results: list of True/False/None (None = skip this observation).
    Returns probability clamped to [0.0001, 0.9999], rounded to 6dp.
    """
    p = float(prior)
    for obs, active in zip(observations, obs_results):
        if active is None:
            continue
        p_t = float(obs["prob_given_true"])
        p_f = float(obs["prob_given_false"])
        if active:
            denom = p * p_t + (1 - p) * p_f
            num = p * p_t
        else:
            denom = p * (1 - p_t) + (1 - p) * (1 - p_f)
            num = p * (1 - p_t)
        if denom == 0:
            continue
        p = num / denom
    return max(0.0001, min(0.9999, round(p, 6)))
