"""Unit tests for patch parsing, whole-batch preconditions and closure math."""
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from meter_lake.patch import (
    FinalStateError,
    PatchError,
    apply_patch,
    dependency_closure,
    read_patch,
    validate_final_state,
)

HEAD = "op,meter_id,observed_at,register_wh,expected_wh\n"
METERS = [
    ("M-A", "North Works", 1, 10),
    ("M-B", "North Works", 1, 5),
    ("M-C", "Canal Pump", 1, 1000),
]
KNOWN = {m[0] for m in METERS}


def dt(text):
    return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc)


def state(pairs):
    return {(meter, dt(stamp)): register for meter, stamp, register in pairs}


class PatchParseTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def patch(self, text, name="patch.csv"):
        path = self.root / name
        path.write_text(HEAD + text)
        return path

    def test_header_only_patch_is_empty_but_valid(self):
        self.assertEqual(read_patch(self.patch("")), [])

    def test_empty_file_without_header_rejected(self):
        path = self.root / "empty.csv"
        path.write_text("")
        with self.assertRaisesRegex(PatchError, "header"):
            read_patch(path)

    def test_wrong_ordered_header_rejected(self):
        path = self.root / "wrong.csv"
        path.write_text("op,meter_id,observed_at,expected_wh,register_wh\n")
        with self.assertRaisesRegex(PatchError, "exact ordered header"):
            read_patch(path)

    def test_duplicate_normalized_key_even_with_different_offset(self):
        path = self.patch(
            "replace,M-A,2026-01-02T00:00:00Z,2500,2000\n"
            "replace,M-A,2026-01-02T01:00:00+01:00,2600,2000\n"
        )
        with self.assertRaisesRegex(PatchError, "duplicate normalized key"):
            read_patch(path)

    def test_illegal_op_field_shape_and_time(self):
        cases = [
            "upsert,M-A,2026-01-02T00:00:00Z,2500,2000\n",
            "insert,M-A,2026-01-02T00:30:00Z,2500,\n",
            "insert,M-A,2026-01-02T00:00:00Z,,\n",
            "delete,M-A,2026-01-02T00:00:00Z,,\n",
            "replace,M-A,2026-01-02T00:00:00Z,,2000\n",
            "replace,M-A,2026-01-02T00:00:00Z,2500,\n",
            "insert,M-A,2026-01-02T00:00:00Z,2500,99\n",
            "delete,M-A,2026-01-02T00:00:00Z,12,2000\n",
        ]
        for case in cases:
            with self.subTest(case=case):
                with self.assertRaises(PatchError):
                    read_patch(self.patch(case))

    def test_timestamp_without_timezone_rejected(self):
        with self.assertRaisesRegex(PatchError, "timezone"):
            read_patch(self.patch("insert,M-A,2026-01-02T00:00:00,100,\n"))


class BatchPreconditionTests(unittest.TestCase):
    def setUp(self):
        self.parent = state([
            ("M-A", "2026-01-01T23:00:00Z", 1000),
            ("M-A", "2026-01-02T00:00:00Z", 2000),
            ("M-A", "2026-01-02T01:00:00Z", 4000),
        ])

    def apply(self, text):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "p.csv"
            path.write_text(HEAD + text)
            rows = read_patch(path)
        return apply_patch(rows, self.parent, KNOWN)

    def test_insert_requires_absent_key(self):
        with self.assertRaisesRegex(PatchError, "insert conflicts") as ctx:
            self.apply("insert,M-A,2026-01-02T00:00:00Z,2500,\n")
        self.assertIn("line 2", str(ctx.exception))

    def test_replace_requires_present_key(self):
        with self.assertRaisesRegex(PatchError, "replace conflicts"):
            self.apply("replace,M-A,2026-01-02T02:00:00Z,5000,4000\n")

    def test_delete_requires_present_key(self):
        with self.assertRaisesRegex(PatchError, "delete conflicts"):
            self.apply("delete,M-A,2026-01-02T02:00:00Z,,4000\n")

    def test_expected_wh_mismatch_is_a_precondition_conflict(self):
        with self.assertRaisesRegex(PatchError, "expected_wh mismatch") as ctx:
            self.apply("replace,M-A,2026-01-02T00:00:00Z,2500,1999\n")
        self.assertIn("line 2", str(ctx.exception))

    def test_unknown_meter_rejected_even_otherwise_well_formed(self):
        with self.assertRaisesRegex(PatchError, "unknown meter"):
            self.apply("insert,M-X,2026-01-02T02:00:00Z,100,\n")

    def test_whole_batch_checks_parent_not_row_order(self):
        # Delete then re-insert the same key in one patch: the preconditions both
        # target the parent, where the key exists, so this must fail rather than
        # being accepted as an ordered mutation.
        with self.assertRaises(PatchError):
            self.apply(
                "delete,M-A,2026-01-02T00:00:00Z,,2000\n"
                "insert,M-A,2026-01-02T00:00:00Z,2500,\n"
            )

    def test_equal_value_replace_is_legal_but_not_a_change(self):
        final, changes, noop = self.apply(
            "replace,M-A,2026-01-02T00:00:00Z,2000,2000\n")
        self.assertEqual(final, self.parent)
        self.assertEqual(changes, [])
        self.assertEqual(len(noop), 1)
        old, new = dependency_closure(changes, self.parent, final)
        self.assertEqual(old, set())
        self.assertEqual(new, set())

    def test_adjacent_revisions_equivalent_in_either_row_order(self):
        order_a = (
            "replace,M-A,2026-01-01T23:00:00Z,2500,1000\n"
            "replace,M-A,2026-01-02T00:00:00Z,3000,2000\n"
        )
        order_b = (
            "replace,M-A,2026-01-02T00:00:00Z,3000,2000\n"
            "replace,M-A,2026-01-01T23:00:00Z,2500,1000\n"
        )
        final_a, changes_a, _ = self.apply(order_a)
        final_b, changes_b, _ = self.apply(order_b)
        self.assertEqual(final_a, final_b)
        self.assertEqual(
            sorted((c["meter_id"], c["observed_at"]) for c in changes_a),
            sorted((c["meter_id"], c["observed_at"]) for c in changes_b),
        )
        self.assertEqual(final_a[("M-A", dt("2026-01-01T23:00:00Z"))], 2500)
        self.assertEqual(final_a[("M-A", dt("2026-01-02T00:00:00Z"))], 3000)


class FinalStateTests(unittest.TestCase):
    # These cases exercise one meter; the dimension stays focused on it.
    DIMENSION = [("M-A", "North Works", 1, 10)]

    def validate(self, pairs):
        validate_final_state(state(pairs), self.DIMENSION)

    def test_minimum_two_boundaries(self):
        with self.assertRaisesRegex(FinalStateError, "at least 2") as ctx:
            self.validate([("M-A", "2026-01-01T00:00:00Z", 1000)])
        self.assertIn("M-A", str(ctx.exception))

    def test_missing_hour_is_final_constraint_failure(self):
        with self.assertRaisesRegex(FinalStateError, "not one hour"):
            self.validate([
                ("M-A", "2026-01-01T00:00:00Z", 1000),
                ("M-A", "2026-01-01T02:00:00Z", 1200),
                ("M-A", "2026-01-01T03:00:00Z", 1300),
            ])

    def test_rollback_is_final_constraint_failure_with_times(self):
        # Per-row intermediate state would dip, but final pair ordering here is
        # itself a rollback; the error must locate meter and interval.
        with self.assertRaisesRegex(FinalStateError, "rolled back") as ctx:
            self.validate([
                ("M-A", "2026-01-01T00:00:00Z", 1000),
                ("M-A", "2026-01-01T01:00:00Z", 900),
                ("M-A", "2026-01-01T02:00:00Z", 2000),
            ])
        message = str(ctx.exception)
        self.assertIn("M-A", message)
        self.assertIn("2026-01-01T00:00:00Z", message)

    def test_capacity_uses_multiplier_and_kw(self):
        with self.assertRaisesRegex(FinalStateError, "capacity"):
            self.validate([
                ("M-A", "2026-01-01T00:00:00Z", 0),
                ("M-A", "2026-01-01T01:00:00Z", 100 * 1000 + 1),
            ])

    def test_zero_usage_is_legal(self):
        self.validate([
            ("M-A", "2026-01-01T00:00:00Z", 500),
            ("M-A", "2026-01-01T01:00:00Z", 500),
        ])

    def test_small_delta_on_huge_cumulative_value(self):
        self.validate([
            ("M-A", "2026-01-01T00:00:00Z", 10**15 - 2),
            ("M-A", "2026-01-01T01:00:00Z", 10**15 - 1),
            ("M-A", "2026-01-01T02:00:00Z", 10**15),
        ])


class ClosureTests(unittest.TestCase):
    def test_single_change_touches_both_adjacent_intervals_on_both_sides(self):
        parent = state([
            ("M-A", "2026-01-01T23:00:00Z", 1000),
            ("M-A", "2026-01-02T00:00:00Z", 2000),
            ("M-A", "2026-01-02T01:00:00Z", 4000),
        ])
        final = dict(parent)
        final[("M-A", dt("2026-01-02T00:00:00Z"))] = 2500
        changes = [{"meter_id": "M-A", "observed_at": dt("2026-01-02T00:00:00Z"),
                    "old_register": 2000, "new_register": 2500}]
        old, new = dependency_closure(changes, parent, final)
        expected = {
            ("M-A", dt("2026-01-01T23:00:00Z"), dt("2026-01-02T00:00:00Z")),
            ("M-A", dt("2026-01-02T00:00:00Z"), dt("2026-01-02T01:00:00Z")),
        }
        self.assertEqual(old, expected)
        self.assertEqual(new, expected)

    def test_adjacent_changes_merge_without_duplicate_intervals(self):
        parent = state([
            ("M-A", "2026-01-01T00:00:00Z", 0),
            ("M-A", "2026-01-01T01:00:00Z", 100),
            ("M-A", "2026-01-01T02:00:00Z", 200),
            ("M-A", "2026-01-01T03:00:00Z", 300),
        ])
        final = state([
            ("M-A", "2026-01-01T00:00:00Z", 0),
            ("M-A", "2026-01-01T01:00:00Z", 150),
            ("M-A", "2026-01-01T02:00:00Z", 250),
            ("M-A", "2026-01-01T03:00:00Z", 300),
        ])
        changes = [
            {"meter_id": "M-A", "observed_at": dt("2026-01-01T01:00:00Z"),
             "old_register": 100, "new_register": 150},
            {"meter_id": "M-A", "observed_at": dt("2026-01-01T02:00:00Z"),
             "old_register": 200, "new_register": 250},
        ]
        old, new = dependency_closure(changes, parent, final)
        self.assertEqual(len(old), 3)
        self.assertEqual(len(new), 3)

    def test_insert_and_delete_sides_are_distinct(self):
        parent = state([
            ("M-A", "2026-01-01T00:00:00Z", 0),
            ("M-A", "2026-01-01T01:00:00Z", 100),
        ])
        # Insert a new later boundary: no old interval, one new interval.
        final = dict(parent)
        final[("M-A", dt("2026-01-01T02:00:00Z"))] = 250
        changes = [{"meter_id": "M-A", "observed_at": dt("2026-01-01T02:00:00Z"),
                    "old_register": None, "new_register": 250}]
        old, new = dependency_closure(changes, parent, final)
        self.assertEqual(old, set())
        self.assertEqual(new, {("M-A", dt("2026-01-01T01:00:00Z"), dt("2026-01-01T02:00:00Z"))})
        # Delete it again: symmetric.
        changes = [{"meter_id": "M-A", "observed_at": dt("2026-01-01T02:00:00Z"),
                    "old_register": 250, "new_register": None}]
        old, new = dependency_closure(changes, final, parent)
        self.assertEqual(old, {("M-A", dt("2026-01-01T01:00:00Z"), dt("2026-01-01T02:00:00Z"))})
        self.assertEqual(new, set())


if __name__ == "__main__":
    unittest.main()
