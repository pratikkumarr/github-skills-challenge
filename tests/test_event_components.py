import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from event_consumer import EventConsumer  # noqa: E402
from event_producer import EventProducer  # noqa: E402
from event_topic import EventTopic  # noqa: E402


def test_event_producer_rejects_empty_events_without_publishing():
    topic = EventTopic("anomaly-events")
    producer = EventProducer(topic)

    assert producer.publish(None) is False
    assert producer.publish({}) is False
    assert topic.get_messages() == []


def test_event_producer_publishes_truthy_event():
    topic = EventTopic("anomaly-events")
    producer = EventProducer(topic)
    event = {"type": "ANOMALY"}

    assert producer.publish(event) is True
    assert topic.get_messages() == [event]


def test_event_topic_returns_copy_and_can_be_cleared():
    topic = EventTopic("anomaly-events")
    event = {"type": "ANOMALY"}
    topic.publish(event)

    messages = topic.get_messages()
    messages.clear()
    assert topic.get_messages() == [event]

    topic.clear()
    assert topic.get_messages() == []


def test_event_consumer_consumes_topic_messages():
    topic = EventTopic("anomaly-events")
    topic.publish({"type": "ANOMALY"})

    assert EventConsumer(topic).consume() == [{"type": "ANOMALY"}]
