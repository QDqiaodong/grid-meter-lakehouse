"""Parse and apply ordered change patches to a cumulative-register state.

A patch is a CSV with the exact ordered header
``op,meter_id,observed_at,register_wh,expected_wh``.  Rows may be unordered;
every precondition is evaluated against the same parent state and the whole
batch is applied as one unit before final meter accounting is checked.  No
floating-point arithmetic is used: register values and deltas stay Python
integers, kWh only becomes a DECIMAL inside SQL.
"""
import csv
from datetime import datetime, timezone

PATCH_HEADERS = ["op", "meter_id", "observed_at", "register_wh", "expected_wh"]
OPS = {"insert", "replace", "delete"}
MAX_REGISTER = 10**15


class PatchError(ValueError):
    """A malformed patch or a precondition conflict against the parent state."""


class FinalStateError(ValueError):
    """The batch is precondition-valid but violates final meter accounting."""


def _parse_instant(text, line_no):
    try:
        instant = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise PatchError(f"patch line {line_no}: invalid ISO8601 timestamp {text!r}") from exc
    if instant.tzinfo is None:
        raise PatchError(f"patch line {line_no}: observed_at requires a timezone")
    instant = instant.astimezone(timezone.utc)
    if instant.minute or instant.second or instant.microsecond:
        raise PatchError(f"patch line {line_no}: observed_at must be a UTC hour boundary")
    return instant


def _parse_int(text, field, line_no, required):
    if text == "":
        if required:
            raise PatchError(f"patch line {line_no}: {field} is required")
        return None
    try:
        value = int(text)
    except ValueError as exc:
        raise PatchError(f"patch line {line_no}: {field} must be an integer") from exc
    if not (0 <= value <= MAX_REGISTER):
        raise PatchError(f"patch line {line_no}: {field} out of cumulative integer range")
    return value


def read_patch(path):
    """Return validated patch rows as (op, meter_id, datetime_utc, reg, exp)."""
    with open(path, newline="", encoding="utf-8") as stream:
        reader = csv.reader(stream)
        try:
            header = next(reader)
        except StopIteration as exc:
            raise PatchError("patch file is empty; an exact header row is required") from exc
        if header != PATCH_HEADERS:
            raise PatchError(f"patch requires exact ordered header {PATCH_HEADERS}, got {header}")
        rows = []
        seen = set()
        for line_no, raw in enumerate(reader, start=2):
            if len(raw) != 5:
                raise PatchError(f"patch line {line_no}: expected 5 fields, got {len(raw)}")
            op, meter, time_text, reg_text, exp_text = (cell.strip() for cell in raw)
            if op not in OPS:
                raise PatchError(f"patch line {line_no}: op must be one of {sorted(OPS)}")
            if not meter:
                raise PatchError(f"patch line {line_no}: meter_id is required")
            instant = _parse_instant(time_text, line_no)
            register = _parse_int(reg_text, "register_wh", line_no, op in {"insert", "replace"})
            expected = _parse_int(exp_text, "expected_wh", line_no, op in {"replace", "delete"})
            if op == "insert" and expected is not None:
                raise PatchError(f"patch line {line_no}: insert leaves expected_wh empty")
            if op == "delete" and register is not None:
                raise PatchError(f"patch line {line_no}: delete leaves register_wh empty")
            key = (meter, instant)
            if key in seen:
                raise PatchError(
                    f"patch line {line_no}: duplicate normalized key {meter}@{instant:%Y-%m-%dT%H:%M:%SZ}"
                )
            seen.add(key)
            rows.append((op, meter, instant, register, expected, line_no))
    return rows


def apply_patch(rows, parent_state, known_meters):
    """Validate preconditions against ``parent_state`` and compute the new state.

    ``parent_state`` maps (meter_id, instant) -> register integer.  Existence and
    expected_wh checks all target that single parent snapshot, independent of
    patch row order.  Returns ``(final_state, changes, noop_replacees)`` where
    ``changes`` lists only keys whose register value actually changed (including
    insertions and deletions) and ``noop_replacees`` lists equal-value replaces.
    """
    final_state = dict(parent_state)
    conflicts = []
    changes = []
    noop_replacees = []
    for op, meter, instant, register, expected, line_no in rows:
        if meter not in known_meters:
            conflicts.append(f"patch line {line_no}: unknown meter_id {meter!r}")
            continue
        key = (meter, instant)
        present = key in parent_state
        stamp = f"{instant:%Y-%m-%dT%H:%M:%SZ}"
        if op == "insert":
            if present:
                conflicts.append(
                    f"patch line {line_no}: insert conflicts, {meter}@{stamp} already exists "
                    f"with register_wh={parent_state[key]}"
                )
                continue
            final_state[key] = register
            changes.append({"meter_id": meter, "observed_at": instant,
                            "old_register": None, "new_register": register})
        elif op == "replace":
            if not present:
                conflicts.append(
                    f"patch line {line_no}: replace conflicts, {meter}@{stamp} does not exist in parent"
                )
                continue
            if parent_state[key] != expected:
                conflicts.append(
                    f"patch line {line_no}: expected_wh mismatch for {meter}@{stamp}: "
                    f"patch says {expected}, parent has {parent_state[key]}"
                )
                continue
            final_state[key] = register
            if register != expected:
                changes.append({"meter_id": meter, "observed_at": instant,
                                "old_register": expected, "new_register": register})
            else:
                noop_replacees.append({"meter_id": meter, "observed_at": instant,
                                       "register_wh": register})
        else:  # delete
            if not present:
                conflicts.append(
                    f"patch line {line_no}: delete conflicts, {meter}@{stamp} does not exist in parent"
                )
                continue
            if parent_state[key] != expected:
                conflicts.append(
                    f"patch line {line_no}: expected_wh mismatch for {meter}@{stamp}: "
                    f"patch says {expected}, parent has {parent_state[key]}"
                )
                continue
            del final_state[key]
            changes.append({"meter_id": meter, "observed_at": instant,
                            "old_register": expected, "new_register": None})
    if conflicts:
        raise PatchError("patch precondition conflicts; no change applied:\n" + "\n".join(conflicts))
    return final_state, changes, noop_replacees


def _meter_series(state, meter):
    points = sorted((instant, register) for (mid, instant), register in state.items() if mid == meter)
    return points


def validate_final_state(state, meters):
    """Check the post-batch accounting contract; collect every violation."""
    violations = []
    dimension = {meter_id: (site, multiplier, max_kw) for meter_id, site, multiplier, max_kw in meters}
    for meter_id in sorted(dimension):
        points = _meter_series(state, meter_id)
        stamp = lambda dt: f"{dt:%Y-%m-%dT%H:%M:%SZ}"
        if len(points) < 2:
            violations.append(f"meter {meter_id}: final state has {len(points)} boundaries, need at least 2")
            continue
        max_kw = dimension[meter_id][2]
        multiplier = dimension[meter_id][1]
        for (prev_at, prev_wh), (at, register) in zip(points, points[1:]):
            if (at - prev_at).total_seconds() != 3600:
                violations.append(
                    f"meter {meter_id}: gap between {stamp(prev_at)} and {stamp(at)} is not one hour"
                )
                continue
            if register < prev_wh:
                violations.append(
                    f"meter {meter_id}: register rolled back from {prev_wh} to {register} "
                    f"between {stamp(prev_at)} and {stamp(at)}"
                )
                continue
            delta = register - prev_wh
            if delta * multiplier > max_kw * 1000:
                violations.append(
                    f"meter {meter_id}: interval {stamp(prev_at)}..{stamp(at)} uses "
                    f"{delta * multiplier} scaled Wh, capacity is {max_kw} kW"
                )
    if violations:
        raise FinalStateError("final meter state violates accounting constraints; batch rejected:\n"
                              + "\n".join(violations))


def neighbors(state, meter, instant):
    """Return (previous, next) (instant, register) points around ``instant``."""
    times = sorted(ts for (mid, ts) in state if mid == meter)
    previous = next_ = None
    for ts in times:
        if ts < instant:
            previous = ts
        elif ts > instant:
            next_ = ts
            break
    prev_point = (previous, state[(meter, previous)]) if previous is not None else None
    next_point = (next_, state[(meter, next_)]) if next_ is not None else None
    return prev_point, next_point


def dependency_closure(changes, parent_state, final_state):
    """Compute old/new candidate interval keys from change-key adjacency.

    A changed boundary contributes the intervals it participates in, in both
    the parent and the final state.  Adjacent changes therefore merge into one
    deduplicated interval set; intervals outside the closure keep parent facts.
    Returns ``(old_intervals, new_intervals)`` with keys
    ``(meter_id, interval_start, interval_end)``.
    """
    old, new = set(), set()

    def add_intervals(bucket, state, meter, instant):
        prev_point, next_point = neighbors(state, meter, instant)
        key = (meter, instant)
        if prev_point is not None:
            bucket.add((meter, prev_point[0], instant))
        if next_point is not None:
            bucket.add((meter, instant, next_point[0]))

    for change in changes:
        meter, instant = change["meter_id"], change["observed_at"]
        # Old side only when the boundary still existed in the parent state.
        if (meter, instant) in parent_state:
            add_intervals(old, parent_state, meter, instant)
        # New side only when the boundary still exists after the batch.
        if (meter, instant) in final_state:
            add_intervals(new, final_state, meter, instant)
    return old, new
