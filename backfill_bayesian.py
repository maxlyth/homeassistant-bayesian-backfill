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


def _solar_elevation(ts, lat_deg, lon_deg):
    """Compute solar elevation angle in degrees at a given Unix timestamp and location.

    Uses the Spencer equation for declination and an equation-of-time correction.
    Accurate to ~1° — sufficient for the >5° threshold used in Bayesian templates.
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


@pyscript_executor
def _load_location(config_dir):
    """Load latitude/longitude from .storage/core.config."""
    path = os.path.join(config_dir, ".storage", "core.config")
    with open(path) as f:
        data = json.load(f)
    d = data.get("data", {})
    return float(d.get("latitude", 0.0)), float(d.get("longitude", 0.0))


@pyscript_executor
def _get_bayesian_entity_ids(pattern, config_dir):
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


@pyscript_executor
def _load_bayesian_config(target_entity_id, config_dir):
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


@pyscript_executor
def _read_purge_keep_days(config_dir, db_path):
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


_WRITE_BATCH_SIZE = 500  # rows per DB commit to avoid blocking HA recorder


@pyscript_executor
def _load_existing_sensor_states(entity_id, start_ts, end_ts, db_path):
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


@pyscript_executor
def _prepare_history_write(entity_id, db_path, min_ts):
    """Look up metadata and base attributes for state history writes.

    Returns (metadata_id, base_attrs, old_state_id) or raises ValueError.
    Opens and closes its own connection — does not hold a lock.
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

        return metadata_id, base_attrs, old_state_id
    finally:
        conn.close()


@pyscript_executor
def _write_history_batch(batch, metadata_id, base_attrs, old_state_id, prev_state_val, db_path):
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
        return inserted, old_state_id, prev_state_val
    finally:
        conn.close()


@pyscript_executor
def _get_bayesian_window_start(entity_ids, db_path):
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


@pyscript_executor
def _load_state_timelines(entity_ids, start_ts, end_ts, load_attrs, db_path):
    """Bulk-load state histories for all entity_ids in the window.

    Queries from (start_ts - 86400) for a 1-day lookback seed for forward-fill.
    Returns {entity_id: {"ts": [...], "state": [...], "attrs": [...]}}, sorted ascending.
    "attrs" entries are raw JSON strings when load_attrs=True, else None.

    Two-step: resolve entity_ids → metadata_ids first so the main query can use
    HA's (metadata_id, last_updated_ts) composite index instead of joining across
    the full states table.
    """
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        placeholders = ",".join("?" * len(entity_ids))
        lookback = start_ts - 86400

        # Step 1: metadata_id → entity_id mapping (indexed lookup)
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


def _get_state_at(timeline, ts):
    """Forward-fill: return the last known state at or before ts, or None."""
    if not timeline or not timeline["ts"]:
        return None
    idx = bisect.bisect_right(timeline["ts"], ts) - 1
    return timeline["state"][idx] if idx >= 0 else None


def _get_attr_at(timeline, ts, attr_name):
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


def _build_jinja2_env(timelines, lat, lon):
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
        state = _get_state_at(tl, ctx["ts"])
        return state if state is not None else "unknown"

    def _state_attr(entity_id, attribute):
        if entity_id == "sun.sun" and attribute == "elevation":
            return _solar_elevation(ctx["ts"], lat, lon)
        tl = timelines.get(entity_id)
        if tl is None:
            return None
        return _get_attr_at(tl, ctx["ts"], attribute)

    def _now():
        return datetime.fromtimestamp(ctx["ts"], tz=timezone.utc)

    env = Environment(undefined=Undefined)
    env.globals.update({"states": _states, "state_attr": _state_attr, "now": _now})
    return env, ctx


def _eval_template(template_obj, ctx):
    """Render a pre-compiled Jinja2 template and return a bool. Returns False on error."""
    try:
        result = template_obj.render().strip().lower()
        return result in ("true", "1", "yes")
    except Exception:
        return False


def _evaluate_observation(obs, timelines, compiled_templates, idx, ctx):
    """Evaluate one observation at ctx["ts"].

    Returns True if active, False if inactive, or None to skip
    (entity has no history at this timestamp — excluded from Bayes update).
    """
    platform = obs.get("platform")

    if platform == "state":
        eid = obs.get("entity_id")
        tl = timelines.get(eid)
        state = _get_state_at(tl, ctx["ts"]) if tl else None
        if state is None:
            return None
        return state == obs.get("to_state")

    if platform == "numeric_state":
        eid = obs.get("entity_id")
        tl = timelines.get(eid)
        raw = _get_state_at(tl, ctx["ts"]) if tl else None
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
        return _eval_template(tpl, ctx)

    return None


def _compute_bayesian_probability(prior, observations, obs_results):
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


async def _backfill_single_bayesian(
    entity_id, cfg, user_start_ts, end_ts, dry_run, debug, db_path, lat, lon,
):
    """Core backfill logic for one Bayesian sensor.

    Returns (count, result_dict) where result_dict contains diff, warnings,
    timing, and sample data for the service response.
    """
    observations = cfg.get("observations", [])
    prior = float(cfg.get("prior", 0.5))
    probability_threshold = float(cfg.get("probability_threshold", 0.5))
    debug_fn = log.warning if (dry_run or debug) else log.info
    empty_result = {"entity_id": entity_id, "events": 0, "warnings": []}

    # Collect all entity_ids needed for state timeline loading
    obs_entity_ids = set()
    for obs in observations:
        if obs.get("platform") in ("state", "numeric_state"):
            if "entity_id" in obs:
                obs_entity_ids.add(obs["entity_id"])
        elif obs.get("platform") == "template":
            tpl_str = obs.get("value_template", "")
            for match in re.findall(
                r"(?:states|state_attr)\(\s*['\"]([^'\"]+)['\"]", tpl_str
            ):
                if match != "sun.sun":
                    obs_entity_ids.add(match)

    obs_entity_ids_list = list(obs_entity_ids)

    load_attrs = False
    for _obs in observations:
        if _obs.get("platform") == "template" and "state_attr(" in _obs.get("value_template", ""):
            load_attrs = True
            break

    # Resolve start_ts (per-sensor when not specified by user)
    if user_start_ts is None:
        if dry_run or debug:
            resolved_start = int(end_ts - 600)
        elif obs_entity_ids_list:
            raw_start = await _get_bayesian_window_start(obs_entity_ids_list, db_path)
            if raw_start is not None:
                resolved_start = int(raw_start)
            else:
                log.warning(
                    "backfill_bayesian: no observation entity history found for %s, "
                    "defaulting to 365 days ago (pass start_offset to override)",
                    entity_id,
                )
                resolved_start = int(time.time() - 365 * 86400)
        else:
            resolved_start = int(time.time() - 365 * 86400)
    else:
        resolved_start = int(user_start_ts)

    if resolved_start >= end_ts:
        log.warning("backfill_bayesian: %s — window is empty, nothing to do", entity_id)
        return 0, empty_result

    # Bulk-load all state timelines (runs in executor thread)
    timelines = {}
    if obs_entity_ids_list:
        timelines = await _load_state_timelines(
            obs_entity_ids_list, resolved_start, end_ts, load_attrs, db_path
        )

    # Collect unique state-change timestamps
    event_timestamps = sorted({
        ts for tl in timelines.values()
        for ts in tl["ts"]
        if resolved_start <= ts < end_ts
    })
    if not event_timestamps:
        event_timestamps = [float(resolved_start)]

    log.warning(
        "backfill_bayesian: %s — window=[%s → %s] "
        "observations=%d entities=%d loaded=%d events=%d",
        entity_id,
        datetime.fromtimestamp(resolved_start, tz=timezone.utc).isoformat(),
        datetime.fromtimestamp(end_ts, tz=timezone.utc).isoformat(),
        len(observations),
        len(obs_entity_ids_list),
        len(timelines),
        len(event_timestamps),
    )

    # Build Jinja2 environment with historical mocks
    env, ctx = _build_jinja2_env(timelines, lat, lon)

    # Pre-compile template observations
    compiled_templates = {}
    for i, obs in enumerate(observations):
        if obs.get("platform") == "template":
            try:
                compiled_templates[i] = env.from_string(obs["value_template"])
            except Exception as exc:
                log.warning(
                    "backfill_bayesian: %s — failed to compile template obs[%d]: %s",
                    entity_id, i, exc,
                )

    # Main loop — iterate over event timestamps, yielding periodically
    t_compute_start = time.time()
    history_rows = []
    debug_logged = 0
    # Track per-observation activation counts for warnings
    obs_active_counts = [0] * len(observations)
    obs_eval_counts = [0] * len(observations)

    for idx, ts in enumerate(event_timestamps):
        ctx["ts"] = float(ts)

        obs_results = [
            _evaluate_observation(obs, timelines, compiled_templates, i, ctx)
            for i, obs in enumerate(observations)
        ]
        prob = _compute_bayesian_probability(prior, observations, obs_results)

        # Track activation counts
        for i, r in enumerate(obs_results):
            if r is not None:
                obs_eval_counts[i] += 1
                if r is True:
                    obs_active_counts[i] += 1

        if debug and debug_logged < 10:
            active_parts = [
                f"obs[{i}]({'T' if r else 'F' if r is not None else '?'})"
                for i, r in enumerate(obs_results)
            ]
            debug_fn(
                "backfill_bayesian [debug] %s %s → prob=%.4f  [%s]",
                entity_id,
                datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(),
                prob,
                " ".join(active_parts),
            )
            debug_logged += 1

        # Build per-timestamp dynamic attributes
        occurred = []
        obs_attrs = []
        for i, obs in enumerate(observations):
            obs_entry = dict(obs)
            if obs_results[i] is True:
                if obs.get("platform") in ("state", "numeric_state"):
                    occurred.append(obs["entity_id"])
                obs_entry.pop("observed", None)
            else:
                obs_entry["observed"] = False
            obs_attrs.append(obs_entry)

        history_rows.append({
            "ts": float(ts),
            "probability": prob,
            "state": "on" if prob >= probability_threshold else "off",
            "occurred_observation_entities": occurred,
            "observations": obs_attrs,
        })

        # Yield every 500 iterations to keep the event loop responsive
        if (idx + 1) % _WRITE_BATCH_SIZE == 0:
            await task.sleep(0)

    computation_seconds = round(time.time() - t_compute_start, 2)
    estimated_write_seconds = round(
        max(1, len(history_rows)) / _WRITE_BATCH_SIZE * 0.1, 2
    )

    # --- Build warnings ---
    warnings = []
    for i, obs in enumerate(observations):
        platform = obs.get("platform", "")
        eid = obs.get("entity_id", "")

        # Template with time-dependent expressions
        if platform == "template":
            tpl = obs.get("value_template", "")
            if "now()" in tpl:
                warnings.append({
                    "type": "template_time_dependent",
                    "observation_index": i,
                    "detail": "Template uses now() — transitions between entity "
                              "state changes are not captured",
                })

        # Missing entity history
        if platform in ("state", "numeric_state") and eid and eid not in timelines:
            warnings.append({
                "type": "missing_entity_history",
                "observation_index": i,
                "entity_id": eid,
                "detail": f"No state history found for {eid} in window",
            })

        # Always/never active
        n_events = len(event_timestamps)
        if n_events > 1 and obs_eval_counts[i] > 0:
            if obs_active_counts[i] == obs_eval_counts[i]:
                warnings.append({
                    "type": "always_active",
                    "observation_index": i,
                    "entity_id": eid,
                    "detail": f"Observation was active for all {obs_eval_counts[i]} evaluated events",
                })
            elif obs_active_counts[i] == 0:
                warnings.append({
                    "type": "never_active",
                    "observation_index": i,
                    "entity_id": eid,
                    "detail": f"Observation was never active across {obs_eval_counts[i]} evaluated events",
                })

    # --- Build diff against existing DB states ---
    existing = await _load_existing_sensor_states(
        entity_id, resolved_start, end_ts, db_path
    )
    # Build lookup: ts → (probability, state) with ±1s tolerance
    existing_by_ts = {}
    for ets, eprob, estate in existing:
        existing_by_ts[round(ets)] = (eprob, estate)

    changed_prob = 0
    changed_state = 0
    new_events = 0
    for row in history_rows:
        key = round(row["ts"])
        match = existing_by_ts.get(key)
        if match is None:
            new_events += 1
        else:
            eprob, estate = match
            if eprob is not None and abs(row["probability"] - eprob) > 0.001:
                changed_prob += 1
            if estate != row["state"]:
                changed_state += 1

    # --- Build sample (first 10 for dry_run) ---
    sample = []
    if dry_run:
        for row in history_rows[:10]:
            key = round(row["ts"])
            match = existing_by_ts.get(key)
            entry = {
                "timestamp": datetime.fromtimestamp(
                    row["ts"], tz=timezone.utc
                ).isoformat(),
                "probability": row["probability"],
                "state": row["state"],
            }
            if match:
                entry["existing_probability"] = match[0]
                entry["existing_state"] = match[1]
            sample.append(entry)

    result = {
        "entity_id": entity_id,
        "events": len(history_rows),
        "window": {
            "start": datetime.fromtimestamp(
                resolved_start, tz=timezone.utc
            ).isoformat(),
            "end": datetime.fromtimestamp(end_ts, tz=timezone.utc).isoformat(),
        },
        "computation_seconds": computation_seconds,
        "estimated_write_seconds": estimated_write_seconds,
        "estimated_total_seconds": round(
            computation_seconds + estimated_write_seconds, 2
        ),
        "diff": {
            "total_events": len(history_rows),
            "existing_states": len(existing),
            "changed_probability": changed_prob,
            "changed_state": changed_state,
            "new_events": new_events,
        },
        "warnings": warnings,
    }
    if sample:
        result["sample"] = sample

    # --- Write state history ---
    if not dry_run and history_rows:
        metadata_id, base_attrs, old_state_id = await _prepare_history_write(
            entity_id, db_path, history_rows[0]["ts"]
        )
        total_inserted = 0
        prev_state_val = None
        for batch_start in range(0, len(history_rows), _WRITE_BATCH_SIZE):
            batch = history_rows[batch_start:batch_start + _WRITE_BATCH_SIZE]
            inserted, old_state_id, prev_state_val = await _write_history_batch(
                batch, metadata_id, base_attrs, old_state_id, prev_state_val, db_path
            )
            total_inserted += inserted
            await task.sleep(0.1)
        log.warning(
            "backfill_bayesian: %s — wrote %d state history row(s)",
            entity_id, total_inserted,
        )
        result["rows_written"] = total_inserted
    elif dry_run and history_rows:
        log.warning(
            "backfill_bayesian [dry_run]: %s — %d events, first=%s last=%s",
            entity_id, len(history_rows),
            datetime.fromtimestamp(history_rows[0]["ts"], tz=timezone.utc).isoformat(),
            datetime.fromtimestamp(history_rows[-1]["ts"], tz=timezone.utc).isoformat(),
        )

    return len(history_rows), result


@service(supports_response="optional")
def backfill_bayesian_sensor(
    target_entity_id,
    start_offset=None,
    end_offset=None,
    dry_run=False,
    debug=False,
):
    """yaml
name: Backfill Bayesian Sensor History
supports_response: optional
description: >
  Retroactively computes Bayesian probability for one or more binary sensors
  using historical state data, then writes the results as state records with
  the probability, occurred_observation_entities, and observations attributes
  so they appear in history graphs and ApexCharts.
  Computes at each observation entity state change (event-driven, not fixed interval).
  Supports glob patterns for target_entity_id (e.g. binary_sensor.bathroom_*
  or binary_sensor.* for all sensors).
  Always do a dry_run first. Re-running is safe — only backfill-written rows
  are replaced.
fields:
  target_entity_id:
    description: >
      Entity ID of the Bayesian sensor, or a glob pattern such as
      binary_sensor.bathroom_* or binary_sensor.* (all Bayesian sensors).
    required: true
    selector:
      text:
  start_offset:
    description: >
      How far back from now to start the backfill window. Leave at 0 to auto-detect
      (uses the earliest observation entity state in the recorder). When dry_run
      or debug is set, 0 defaults to a 10-minute window instead.
    required: true
    default:
      days: 0
      hours: 0
      minutes: 0
      seconds: 0
    selector:
      duration:
  end_offset:
    description: >
      How far back from now to end the backfill window. Leave at 0 for now.
      Re-running is safe — only backfill-written rows are replaced.
    required: true
    default:
      days: 0
      hours: 0
      minutes: 0
      seconds: 0
    selector:
      duration:
  dry_run:
    description: If true, log computed values but do not write to the database.
    required: false
    default: false
    selector:
      boolean:
  debug:
    description: >
      If true, log a per-observation breakdown for the first 10 timestamps.
      Useful for validating observation config and checking probability values.
    required: false
    default: false
    selector:
      boolean:
"""
    task.unique("backfill_bayesian_sensor")

    config_dir = hass.config.config_dir
    db_path = os.path.join(config_dir, "home-assistant_v2.db")

    # Load location for solar elevation computation
    lat, lon = await _load_location(config_dir)

    # Convert duration offsets to Unix timestamps
    now_ts = int(time.time())

    def _offset_to_ts(offset):
        if offset is None or offset == "~":
            return None
        if isinstance(offset, str):
            # Parse Python timedelta str: [D day[s], ]H:MM:SS
            m = re.fullmatch(r"(?:(\d+) days?,\s*)?(\d+):(\d+):(\d+)", offset.strip())
            if not m:
                raise ValueError(
                    f"Cannot parse offset {offset!r}; expected dict or 'HH:MM:SS' string"
                )
            d, h, mi, s = [int(x or 0) for x in m.groups()]
            secs = d * 86400 + h * 3600 + mi * 60 + s
        else:
            secs = (
                int(offset.get("days", 0)) * 86400
                + int(offset.get("hours", 0)) * 3600
                + int(offset.get("minutes", 0)) * 60
                + int(offset.get("seconds", 0))
            )
        # Zero total means "use default behaviour" (auto-detect start / now for end)
        if secs == 0:
            return None
        return now_ts - secs

    start_ts = _offset_to_ts(start_offset)
    end_ts = _offset_to_ts(end_offset)

    # Resolve end_ts once (shared across all sensors in a glob expansion)
    if end_ts is None:
        end_ts = now_ts

    # Expand glob / validate entity_id
    entity_ids = await _get_bayesian_entity_ids(target_entity_id, config_dir)
    if not entity_ids:
        log.error(
            "backfill_bayesian: no Bayesian sensors matched %r — "
            "use a glob like 'binary_sensor.*' or check the entity registry",
            target_entity_id,
        )
        return

    log.warning(
        "backfill_bayesian: matched %d sensor(s): %s%s",
        len(entity_ids),
        entity_ids[:5],
        " ..." if len(entity_ids) > 5 else "",
    )

    total_sensors = 0
    total_rows = 0
    sensor_results = {}

    for eid in entity_ids:
        try:
            cfg = await _load_bayesian_config(eid, config_dir)
        except ValueError as exc:
            log.error("backfill_bayesian: %s", exc)
            continue

        rows, result = await _backfill_single_bayesian(
            eid, cfg, start_ts, end_ts, dry_run, debug, db_path, lat, lon,
        )
        log.warning(
            "backfill_bayesian: %s — complete, computed=%d rows, dry_run=%s",
            eid, rows, dry_run,
        )
        total_sensors += 1
        total_rows += rows
        sensor_results[eid] = result

    log.warning(
        "backfill_bayesian: finished — %d sensor(s) processed, %d total rows",
        total_sensors,
        total_rows,
    )

    return {
        "sensors_processed": total_sensors,
        "total_rows": total_rows,
        "dry_run": dry_run,
        "sensors": sensor_results,
    }
