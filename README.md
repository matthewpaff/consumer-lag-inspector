# Consumer Lag Inspector

Inspect supplied partition offsets and summarize consumer lag without connecting to a broker.

Uses Python 3.10+ and its standard library only. No package installation, virtual
environment, external server, Redis, Celery, or network access is required.

## Run

```console
python -m lag_lab --help
python -m lag_lab examples/sample.json
python -m lag_lab --check-fixtures fixtures/cases.jsonl
python generate_fixtures.py --count 20
python -m unittest discover -s tests -v
```

Run from this directory. The optional path defaults to `-`, which reads JSON from
stdin. Output is JSON or a fixture-check summary. Exit codes are 0 success,
1 file failure, and 2 invalid input. Examples and fixtures contain generated data.

Input contains partitions and optional warn_at (default 100). Every partition requires topic, partition, high_watermark, and committed; both offsets are supplied next offsets. Lag is high_watermark minus committed, and committed must not exceed the watermark. Topics are trimmed ASCII letters, digits, dots, underscores, or hyphens, with 1-80 characters; partition IDs are 0-10000. Offsets and warning thresholds are nonnegative integers up to 10^15; booleans and duplicate topic/partition pairs are rejected. Results sort partitions by topic and ID, aggregate per-topic and global lag, and count warnings. Zero lag is always caught_up, positive lag at or above warn_at is warning, and other positive lag is lagging. At most 1000 partition records are accepted. Empty input is valid. No offsets are fetched or committed, and this is not a Kafka administration client.

## Learning focus

The modeled 2017 Kafka operations theme: partition offsets, deterministic consumer-lag summaries, and threshold diagnostics. This is an independent concept exercise based on the modeled
roadmap. It makes no claim to be employer-owned or period-authentic production code.

## Structure

- `lag_lab/core.py`: validated parsing, storage, or state transitions.
- `lag_lab/cli.py`: JSON command interface.
- `lag_lab/generate_fixtures.py`: original deterministic validation cases.
- `lag_lab/fixture_check.py`: expected-output/error checks.
- `fixtures/cases.jsonl`: 456 generated cases, with explicit descriptions.
- `tests/`: independently selected boundaries and the complete corpus check.

Branches `feature/core`, `feature/cli`, `data/fixtures`, and `docs/usage` retain
snapshots; `main` contains the complete tool. Dates and provenance are also recorded
in TIMELINE.json. Many data cases share a scenario; they are not separate features.

Keep changes focused, use actual dates for new work, and add a regression test
for changed behavior. Licensed under [MIT](LICENSE).

[History and provenance](HISTORY.md).
