import json

from litecast.events import emit_event, flush_events


def test_events_are_buffered_and_flushed_once(monkeypatch, tmp_path):
    flush_events()
    monkeypatch.setenv("LITECAST_EVENT_DIR", str(tmp_path))
    monkeypatch.setenv("LITECAST_ROLE", "client")
    monkeypatch.setenv("LITECAST_NODE_ID", "node-1")

    emit_event("one", value=1)
    emit_event("two", value=2)
    assert list(tmp_path.iterdir()) == []

    flush_events()
    paths = list(tmp_path.glob("*.jsonl"))
    assert len(paths) == 1
    records = [json.loads(line) for line in paths[0].read_text().splitlines()]
    assert [(record["event"], record["value"]) for record in records] == [
        ("one", 1),
        ("two", 2),
    ]

    flush_events()
    assert len(paths[0].read_text().splitlines()) == 2
