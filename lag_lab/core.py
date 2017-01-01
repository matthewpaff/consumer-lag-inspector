"""Inspect supplied next offsets without connecting to a broker."""
import re


def integer(value, label, low, high):
    if type(value) is not int or not low <= value <= high:
        raise ValueError("{} must be an integer from {} to {}".format(label, low, high))
    return value


def inspect_lag(data):
    if not isinstance(data, dict) or not {"partitions"} <= set(data) <= {"partitions", "warn_at"}:
        raise ValueError("Supply partitions and optional warn_at")
    supplied = data["partitions"]
    if not isinstance(supplied, list) or len(supplied) > 1000:
        raise ValueError("Supply at most 1000 partition records")
    threshold = integer(data.get("warn_at", 100), "warn_at", 0, 10 ** 15)
    partitions, seen = [], set()
    for item in supplied:
        if not isinstance(item, dict) or set(item) != {"topic", "partition", "high_watermark", "committed"}:
            raise ValueError("Each partition requires topic, partition, high_watermark, and committed")
        topic = item["topic"]
        if not isinstance(topic, str) or not re.fullmatch(r"[A-Za-z0-9._-]{1,80}", topic.strip()) or topic.strip() in (".", ".."):
            raise ValueError("Topic must contain 1-80 ASCII letters, digits, dots, underscores, or hyphens")
        topic = topic.strip()
        number = integer(item["partition"], "partition", 0, 10000)
        high = integer(item["high_watermark"], "high_watermark", 0, 10 ** 15)
        committed = integer(item["committed"], "committed", 0, 10 ** 15)
        if committed > high:
            raise ValueError("Committed next offset must not exceed the high watermark")
        pair = (topic, number)
        if pair in seen:
            raise ValueError("Topic and partition pairs must be unique")
        seen.add(pair)
        lag = high - committed
        status = "caught_up" if lag == 0 else "warning" if lag >= threshold else "lagging"
        partitions.append({"topic": topic, "partition": number, "high_watermark": high,
                           "committed": committed, "lag": lag, "status": status})
    partitions.sort(key=lambda row: (row["topic"], row["partition"]))
    grouped = {}
    for row in partitions:
        group = grouped.setdefault(row["topic"], {"topic": row["topic"], "partitions": 0,
                                                "total_lag": 0, "max_lag": 0, "warning_partitions": 0})
        group["partitions"] += 1
        group["total_lag"] += row["lag"]
        group["max_lag"] = max(group["max_lag"], row["lag"])
        group["warning_partitions"] += int(row["status"] == "warning")
    topics = [grouped[topic] for topic in sorted(grouped)]
    return {"partitions": partitions, "topics": topics,
            "total_lag": sum(row["lag"] for row in partitions),
            "warning_partitions": sum(row["status"] == "warning" for row in partitions)}
