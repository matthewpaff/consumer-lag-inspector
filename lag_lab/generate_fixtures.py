"""Original generated offset examples; these are not actual broker observations."""


def supplied(topic, partition, high, committed):
    return {"topic": topic, "partition": partition, "high_watermark": high, "committed": committed}


def observed(topic, partition, high, lag, status):
    return {"topic": topic, "partition": partition, "high_watermark": high,
            "committed": high - lag, "lag": lag, "status": status}


def topic_total(topic, count, lag, maximum, warnings):
    return {"topic": topic, "partitions": count, "total_lag": lag,
            "max_lag": maximum, "warning_partitions": warnings}


def make_case(index):
    topic, high, kind = "reviews-{}".format(index), index * 100 + 200, index % 12
    data = {"warn_at": 10, "partitions": [supplied(topic, 0, high, high)]}
    if kind == 0:
        data["partitions"][0]["partition"] = True
        description = "reject booleans as partition numbers"
    elif kind == 1:
        data["partitions"][0]["committed"] = -1
        description = "reject negative committed offsets"
    elif kind == 2:
        data["partitions"][0]["committed"] = high + 1
        description = "reject consumer offsets beyond the supplied watermark"
    elif kind == 3:
        data["partitions"].append(supplied(topic, 0, high, high))
        description = "reject duplicate topic and partition pairs"
    elif kind == 4:
        data["partitions"][0]["topic"] = "   "
        description = "reject blank topic names"
    elif kind == 5:
        data["warn_at"] = -1
        description = "reject negative lag warning thresholds"
    elif kind == 6:
        data["partitions"] = []
        expected = {"partitions": [], "topics": [], "total_lag": 0, "warning_partitions": 0}
        description = "summarize an empty partition collection"
    elif kind == 7:
        data["warn_at"] = 0
        expected = {"partitions": [observed(topic, 0, high, 0, "caught_up")],
                    "topics": [topic_total(topic, 1, 0, 0, 0)], "total_lag": 0, "warning_partitions": 0}
        description = "keep zero-lag partitions caught up at a zero warning threshold"
    elif kind in (8, 9):
        lag = 1 + index % 13
        warnings = int(kind == 9)
        data.update({"warn_at": lag if warnings else lag + 1,
                     "partitions": [supplied(topic, 0, high, high - lag)]})
        expected = {"partitions": [observed(topic, 0, high, lag, "warning" if warnings else "lagging")],
                    "topics": [topic_total(topic, 1, lag, lag, warnings)], "total_lag": lag, "warning_partitions": warnings}
        description = "warn exactly at the configured lag threshold" if warnings else "retain positive lag below the warning threshold"
    elif kind == 10:
        first, second = "topic-a-{}".format(index), "topic-b-{}".format(index)
        lag_a, lag_b = 2 + index % 5, 3 + index % 7
        warn_a, warn_b = int(lag_a >= 4), int(lag_b >= 4)
        data = {"warn_at": 4, "partitions": [supplied(second, 0, high, high - lag_b), supplied(first, 0, high, high - lag_a)]}
        expected = {"partitions": [observed(first, 0, high, lag_a, "warning" if warn_a else "lagging"),
                                   observed(second, 0, high, lag_b, "warning" if warn_b else "lagging")],
                    "topics": [topic_total(first, 1, lag_a, lag_a, warn_a), topic_total(second, 1, lag_b, lag_b, warn_b)],
                    "total_lag": lag_a + lag_b, "warning_partitions": warn_a + warn_b}
        description = "aggregate independent topics in deterministic order"
    else:
        lag = 10 + index % 17
        data["partitions"] = [supplied(topic, 1, high, high - lag), supplied(topic, 0, high, high)]
        expected = {"partitions": [observed(topic, 0, high, 0, "caught_up"), observed(topic, 1, high, lag, "warning")],
                    "topics": [topic_total(topic, 2, lag, lag, 1)], "total_lag": lag, "warning_partitions": 1}
        description = "combine caught-up and warning partitions within one topic"
    result = {"id": "lag-{:04d}".format(index), "description": description, "input": data}
    result.update({"expected_error": True} if kind < 6 else {"expected": expected})
    return result
