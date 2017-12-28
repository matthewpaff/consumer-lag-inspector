from copy import deepcopy
from pathlib import Path
import unittest

from lag_lab.core import inspect_lag


class LagTests(unittest.TestCase):
    def test_groups_topics_and_sorts_partitions_without_mutating_input(self):
        data = {"warn_at": 5, "partitions": [
            {"topic": "b", "partition": 1, "high_watermark": 30, "committed": 20},
            {"topic": "a", "partition": 0, "high_watermark": 10, "committed": 7},
            {"topic": "b", "partition": 0, "high_watermark": 20, "committed": 20}]}
        original = deepcopy(data)
        output = inspect_lag(data)
        self.assertEqual(data, original)
        self.assertEqual([(row["topic"], row["partition"]) for row in output["partitions"]], [("a", 0), ("b", 0), ("b", 1)])
        self.assertEqual((output["total_lag"], output["warning_partitions"]), (13, 1))
        self.assertEqual(output["topics"][1], {"topic": "b", "partitions": 2, "total_lag": 10, "max_lag": 10, "warning_partitions": 1})

    def test_warning_boundaries_and_zero_threshold(self):
        record = {"topic": "reviews", "partition": 0, "high_watermark": 10, "committed": 5}
        self.assertEqual(inspect_lag({"partitions": [record], "warn_at": 5})["partitions"][0]["status"], "warning")
        self.assertEqual(inspect_lag({"partitions": [record], "warn_at": 6})["partitions"][0]["status"], "lagging")
        record["committed"] = 10
        self.assertEqual(inspect_lag({"partitions": [record], "warn_at": 0})["partitions"][0]["status"], "caught_up")

    def test_invalid_offsets_duplicate_pairs_and_boolean_values(self):
        record = {"topic": "reviews", "partition": 0, "high_watermark": 10, "committed": 5}
        invalid = [
            {"partitions": [record, dict(record)]},
            {"partitions": [{**record, "committed": 11}]},
            {"partitions": [{**record, "committed": True}]},
            {"partitions": [{**record, "partition": -1}]},
            {"partitions": [record], "warn_at": False},
            {"partitions": [{"topic": "reviews"}]},
        ]
        for data in invalid:
            with self.subTest(data=data), self.assertRaises(ValueError):
                inspect_lag(data)

    def test_empty_input(self):
        self.assertEqual(inspect_lag({"partitions": []}), {"partitions": [], "topics": [], "total_lag": 0, "warning_partitions": 0})

    def test_complete_fixture_corpus(self):
        from lag_lab.fixture_check import check
        self.assertGreaterEqual(check(Path(__file__).resolve().parents[1] / "fixtures/cases.jsonl", inspect_lag), 350)


if __name__ == "__main__":
    unittest.main()
