import uuid
from datetime import datetime
import pytest
from unittest.mock import Mock
from models.aggregate import (
    EventAggregate,
    AggregateType,
    EventAggregateStore,
    AggregationError,
    EventAggregateConfig,
)
from models.event import (
    Event,
)


def test_event_aggregate_config_count_no_field():
    config = EventAggregateConfig(
        type=AggregateType.COUNT, name="count_aggregate", event_name="some_event"
    )
    assert config.field is None


def test_event_aggregate_config_count_with_field():
    with pytest.raises(
        ValueError, match="Field is not required for COUNT aggregate type."
    ):
        EventAggregateConfig(
            type=AggregateType.COUNT,
            name="count_aggregate",
            event_name="some_event",
            field="some_field",
        )


def test_event_aggregate_config_sum_without_field():
    with pytest.raises(
        ValueError, match="Field is required for SUM or DISTINCT_COUNT aggregate type."
    ):
        EventAggregateConfig(
            type=AggregateType.SUM, name="sum_aggregate", event_name="some_event"
        )


def test_event_aggregate_config_sum_with_field():
    config = EventAggregateConfig(
        type=AggregateType.SUM,
        name="sum_aggregate",
        event_name="some_event",
        field="some_field",
    )
    assert config.field == "some_field"

def test_event_aggregate_count():
    # Create an EventAggregate of type COUNT
    aggregate = EventAggregate(
        name="count_aggregate", event_name="test_event", type=AggregateType.COUNT
    )

    # Create events with user_id
    user_id = "user_1"
    event_properties = {}
    event = Event(
        uuid=uuid.uuid4(),
        name="test_event",
        timestamp=datetime.now(),
        event_properties=event_properties,
    )

    # Update the aggregate with the event
    aggregate.update(user_id=user_id, event=event)
    value = aggregate.get_user_aggregate(user_id=user_id)
    assert value == 1

    # Update again
    aggregate.update(user_id=user_id, event=event)
    value = aggregate.get_user_aggregate(user_id=user_id)
    assert value == 2

    # Test with another user
    user_id_2 = "user_2"
    event_properties_2 ={}
    event_2 = Event(
        uuid=uuid.uuid4(),
        name="test_event",
        timestamp=datetime.now(),
        event_properties=event_properties_2,
    )

    aggregate.update(user_id=user_id_2, event=event_2)
    value_2 = aggregate.get_user_aggregate(user_id=user_id_2)
    assert value_2 == 1


def test_event_aggregate_sum_with_mock_properties():
    aggregate = EventAggregate(
        name="sum_aggregate",
        event_name="chargeback",
        type=AggregateType.SUM,
        field="amount",
    )

    mock_event_1 = Mock()
    mock_event_1.event_properties = Mock(amount=100.0)

    mock_event_2 = Mock()
    mock_event_2.event_properties = Mock(amount=50.0)

    mock_event_3 = Mock()
    mock_event_3.event_properties = Mock(amount=200.0)

    user_id = "user_1"

    # Update aggregate with the first mock event
    aggregate.update(user_id=user_id, event=mock_event_1)
    value = aggregate.get_user_aggregate(user_id=user_id)
    assert value == 100.0

    # Update again with the second mock event
    aggregate.update(user_id=user_id, event=mock_event_2)
    value = aggregate.get_user_aggregate(user_id=user_id)
    assert value == 150.0

    # Test with another user and the third mock event
    user_id_2 = "user_2"
    aggregate.update(user_id=user_id_2, event=mock_event_3)
    value_2 = aggregate.get_user_aggregate(user_id=user_id_2)
    assert value_2 == 200.0