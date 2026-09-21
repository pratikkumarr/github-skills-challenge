import json
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from aiops_pipeline import load_data, run_pipeline  # noqa: E402
from anomaly_detector import AnomalyDetector  # noqa: E402
from calculations import area_of_circle, get_nth_fibonacci  # noqa: E402
from event_consumer import EventConsumer  # noqa: E402
from event_producer import EventProducer  # noqa: E402
from event_topic import EventTopic  # noqa: E402


def record(**overrides):
    value = {
        "timestamp": "2026-09-20T10:00:00",
        "service": "payment-service",
        "response_time_ms": 100,
        "cpu_percent": 20,
        "memory_percent": 30,
        "log_level": "INFO",
        "message": "ok",
    }
    value.update(overrides)
    return value


def test_anomaly_detector_detects_each_threshold_and_warning():
    detector = AnomalyDetector(response_time_threshold=10, cpu_threshold=20, memory_threshold=30)

    event = detector.detect(
        record(response_time_ms=11, cpu_percent=21, memory_percent=31, log_level="WARNING")
    )

    assert event["type"] == "ANOMALY"
    assert event["reasons"] == [
        "High response time",
        "High CPU utilization",
        "High memory utilization",
        "Error log detected",
    ]
    assert event["source"]["service"] == "payment-service"


def test_anomaly_detector_returns_none_when_record_is_normal():
    assert AnomalyDetector().detect(record()) is None


def test_event_producer_rejects_falsy_events():
    topic = EventTopic("events")
    producer = EventProducer(topic)

    assert producer.publish(None) is False
    assert producer.publish({}) is False
    assert topic.get_messages() == []


def test_event_topic_copy_and_clear():
    topic = EventTopic("events")
    event = {"id": 1}
    topic.publish(event)

    messages = topic.get_messages()
    messages.clear()
    assert topic.get_messages() == [event]

    topic.clear()
    assert topic.get_messages() == []


def test_event_consumer_returns_topic_messages():
    topic = EventTopic("events")
    topic.publish({"id": 1})

    assert EventConsumer(topic).consume() == [{"id": 1}]


def test_pipeline_loads_data_and_reports_detected_events(tmp_path):
    data = [
        record(),
        record(response_time_ms=600, log_level="ERROR"),
    ]
    path = tmp_path / "service_data.json"
    path.write_text(json.dumps(data), encoding="utf-8")

    assert load_data(path) == data
    result = run_pipeline(path)

    assert result["records_processed"] == 2
    assert len(result["anomalies_detected"]) == 1
    # The current pipeline intentionally uses a separate consumer topic.
    assert result["events_consumed"] == []


def test_area_of_circle_rejects_negative_radius():
    with pytest.raises(ValueError, match="Radius cannot be negative"):
        area_of_circle(-1)


def test_get_nth_fibonacci_calculates_recursive_case():
    assert get_nth_fibonacci(10) == 55


def test_get_nth_fibonacci_rejects_negative_n():
    with pytest.raises(ValueError, match="n cannot be negative"):
        get_nth_fibonacci(-1)
