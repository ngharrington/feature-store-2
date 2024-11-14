# app_builder.py

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Dict, List

from config import (
    DEFAULT_AGGREGATE_CONFIG_DICT,
    DEFAULT_FEATURES_CONFIG_DICT,
    DEFAULT_RULE_CONFIG_DICT,
    ConfigError,
    get_aggregate_configs,
    get_event_properties_map,
)
from models.aggregate import (
    AggregateType,
    EventAggregate,
    EventAggregateConfig,
    EventAggregateStore,
)
from models.rules import (
    PlatformFeature,
    Rule,
    RuleCondition,
    RuleOperation,
    RulesStore,
)
from services.event_processer import EventConsumer, EventProcessor
from services.event_registry import EventSchemaRegistry, EventTypeNotRegistered
from services.feature_registry import PlatformFeaturesRegistry
from services.notifications import NotificationsService
from services.user_feature import UserFeatureService

NUM_CONSUMERS = 3
event_queue = asyncio.Queue()


def configure_logger():
    logger = logging.getLogger("user_feature_service")
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


def initialize_schema_registry():
    event_schema_registry = EventSchemaRegistry()
    event_properties_map = get_event_properties_map()
    for event_name, event_properties in event_properties_map.items():
        event_schema_registry.register_event_properties_schema(
            event_name, event_properties
        )
    return event_schema_registry


async def build_aggregates(
    aggregate_configs: List[EventAggregateConfig], schema_registry: EventSchemaRegistry
) -> List[EventAggregate]:
    aggregates = []
    for config in aggregate_configs:
        try:
            event_schema = await schema_registry.get_schema_by_name(config.event_name)
            if config.field and config.field not in event_schema.model_fields:
                raise ConfigError(
                    f"Field '{config.field}' not found in event properties schema for event '{config.event_name}'"
                )
            agg = EventAggregate(
                name=config.name,
                event_name=config.event_name,
                type=AggregateType(config.type),
                field=config.field,
            )
            aggregates.append(agg)
        except EventTypeNotRegistered as e:
            raise ConfigError(str(e))
    return aggregates


async def build_aggregate_store(
    aggregate_configs: List[EventAggregateConfig], schema_registry: EventSchemaRegistry
) -> EventAggregateStore:
    aggregates = await build_aggregates(aggregate_configs, schema_registry)
    aggregate_store = EventAggregateStore()
    for agg in aggregates:
        aggregate_store.add_aggregate(agg)
    return aggregate_store


async def build_rule_store(
    rules_config: List[Dict[str, str]], aggregate_store: EventAggregateStore
) -> RulesStore:
    rules_store = RulesStore()
    for config in rules_config:
        aggregate1 = await aggregate_store.get_aggregate_by_name(config["aggregate1"])
        aggregate2 = (
            None
            if not config.get("aggregate2")
            else await aggregate_store.get_aggregate_by_name(config["aggregate2"])
        )
        condition = RuleCondition(config["condition"])
        operation = RuleOperation(config["operation"])
        rule = Rule(
            name=config["name"],
            operation=operation,
            aggregate1=aggregate1,
            aggregate2=aggregate2,
            value=config["value"],
            condition=condition,
            denom_min=config.get("denom_min"),
        )
        rules_store.add_rule(rule)
    return rules_store


async def build_platform_feature_registry(
    feature_config: List[Dict[str, List[str]]], rules_store: RulesStore
) -> PlatformFeaturesRegistry:
    feature_registry = PlatformFeaturesRegistry()
    for config in feature_config:
        rules = []
        for rule_name in config["rules"]:
            rule = await rules_store.get_rule_by_name(rule_name)
            rules.append(rule)
        feature = PlatformFeature(name=config["name"], rules=rules)
        feature_registry.add_feature(feature)
    return feature_registry


@asynccontextmanager
async def lifespan(app):
    logger = configure_logger()
    # Initialize schema registry
    schema_registry = initialize_schema_registry()

    # Load configurations
    aggregate_configs = get_aggregate_configs(DEFAULT_AGGREGATE_CONFIG_DICT)
    rules_config_dict = DEFAULT_RULE_CONFIG_DICT

    # Build components
    aggregate_store = await build_aggregate_store(aggregate_configs, schema_registry)
    rules_store = await build_rule_store(rules_config_dict, aggregate_store)
    feature_registry = await build_platform_feature_registry(
        DEFAULT_FEATURES_CONFIG_DICT, rules_store
    )
    notifications_service = NotificationsService()
    user_feature_service = UserFeatureService(
        feature_registry=feature_registry,
        notifications_service=notifications_service,
        logger=logger,
    )
    event_processor = EventProcessor(
        aggregate_store=aggregate_store,
        rule_store=rules_store,
        feature_registry=feature_registry,
        user_feature_service=user_feature_service,
        logger=logger,
    )

    # Attach components to app state
    app.state.user_feature_service = user_feature_service
    app.state.feature_registry = feature_registry
    app.state.event_queue = event_queue
    app.state.schema_registry = schema_registry
    app.state.logger = logger

    consumer = EventConsumer(
        queue=event_queue, event_processor=event_processor, logger=logger
    )
    consumer_tasks = [
        asyncio.create_task(consumer.consume()) for _ in range(NUM_CONSUMERS)
    ]
    circuit_breaker_task = asyncio.create_task(
        user_feature_service.evaluate_circuit_breakers()
    )

    yield

    await event_queue.join()
    circuit_breaker_task.cancel()
    for task in consumer_tasks:
        task.cancel()
    await asyncio.gather(*consumer_tasks, return_exceptions=True)
