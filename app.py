from fastapi import FastAPI, HTTPException, status, Depends, Header
from config import get_event_properties_map, ConfigError
from services.event_registry import EventSchemaRegistry, EventTypeNotRegistered
from services.event_processer import EventProcessor, EventConsumer
from services.feature_registry import PlatformFeaturesRegistry
from config import get_aggregate_configs, DEFAULT_AGGREGATE_CONFIG_DICT, DEFAULT_RULE_CONFIG_DICT, DEFAULT_FEATURES_CONFIG_DICT
from models.event import Event
from models.aggregate import EventAggregateConfig, EventAggregate, EventAggregateStore, AggregateType
from models.rules import RulesStore, RuleCondition, RuleOperation, PlatformFeature, Rule
import asyncio
from contextlib import asynccontextmanager
from typing import List, Dict
from services.user_feature import UserFeatureService
import re


NUM_CONSUMERS = 3

event_queue = asyncio.Queue()


def _initialize_schema_registry():
    print('initializing')
    event_schema_registry = EventSchemaRegistry()
    event_properties_map = get_event_properties_map()
    for event_name, event_properties in event_properties_map.items():
        event_schema_registry.register_event_properties_schema(event_name, event_properties)
    return event_schema_registry

schema_registry = _initialize_schema_registry()
aggregate_configs = get_aggregate_configs(DEFAULT_AGGREGATE_CONFIG_DICT)
rules_config_dict = DEFAULT_RULE_CONFIG_DICT

async def build_aggregates(
        aggregate_config: Dict[str, List[EventAggregateConfig]],
        schema_registry: EventSchemaRegistry 
        ) -> List[EventAggregate]:
    aggregates = []
    for config in aggregate_config:
        try:
            event_schema = await schema_registry.get_schema_by_name(config.event_name)
            if config.field:
                if not config.field in event_schema.model_fields:
                    raise ConfigError(f"Field '{config.field}' not found in event properties schema for event '{config.event_name}'")
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

async def build_aggregate_store(aggregate_config, schema_registry):
    aggregates = await build_aggregates(aggregate_config, schema_registry)
    aggregate_store = EventAggregateStore()
    for agg in aggregates:
        aggregate_store.add_aggregate(agg)
    return aggregate_store
        

async def build_rule_store(
    rules_config: List[Dict[str, str]], aggregate_store: EventAggregateStore
):
    rules_store = RulesStore()
    for config in rules_config:
        aggregate1 = await aggregate_store.get_aggregate_by_name(config["aggregate1"])
        aggregate2 = (
            None
            if not config["aggregate2"]
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
    feature_config: List[Dict[str, List[str]]], rules_store: RulesStore):
    feature_registry = PlatformFeaturesRegistry()
    for config in feature_config:
        rules = []
        for rule_name in config["rules"]:
            rule = await rules_store.get_rule_by_name(rule_name)
            rules.append(rule)
        feature = PlatformFeature(
            name=config["name"],
            rules=rules
        )
        feature_registry.add_feature(feature)
    return feature_registry

@asynccontextmanager
async def lifespan(app: FastAPI):
    aggregate_store = await build_aggregate_store(aggregate_configs, schema_registry)
    rules_store = await build_rule_store(rules_config_dict, aggregate_store)
    feature_registry = await build_platform_feature_registry(DEFAULT_FEATURES_CONFIG_DICT, rules_store)
    user_feature_service = UserFeatureService(feature_registry=feature_registry)
    app.state.user_feature_service = user_feature_service
    app.state.feature_registry = feature_registry
    event_processor = EventProcessor(
        aggregate_store=aggregate_store,
        rule_store=rules_store,
        feature_registry=feature_registry,
        user_feature_service=user_feature_service,
    )
    consumer = EventConsumer(
        queue=event_queue,
        event_processor=event_processor
    )
    consumer_tasks = [asyncio.create_task(consumer.consume()) for _ in range(NUM_CONSUMERS)]
    yield

    await event_queue.join()
    for task in consumer_tasks:
        task.cancel()
    await asyncio.gather(*consumer_tasks, return_exceptions=True)

app = FastAPI(lifespan=lifespan)


@app.get("/")
async def read_root():
    return {"Hello": "World"}


@app.post("/event")
async def publish_event(event: Event):
    try:
        event_properties_schema = await schema_registry.get_schema_by_name(event.name)
    except EventTypeNotRegistered as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
    
    if not isinstance(event.event_properties, dict):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid event format for event type {event.name}")
    
    parsed_event = Event(
        uuid=event.uuid,
        name=event.name,
        timestamp=event.timestamp,
        event_properties=event_properties_schema(**event.event_properties)
    )

    await event_queue.put(parsed_event)
    return {"event_id": event.uuid}

@app.get("/queue-size")
async def get_queue_size():
    """
    Endpoint to return the current size of the event queue.
    """
    try:
        queue_size = event_queue.qsize()
        return {"queue_size": queue_size}
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get queue size: {str(e)}"
        )
    
@app.get("/{feature_flag}")
async def can_access_feature(feature_flag: str, x_user_id: str = Header(...)):
    # check if format is of the name "can<feature_name>" where featurename is lowercase ascii
    # for simplicity
    if not re.match(r"^can[a-z]{1,16}+$", feature_flag):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid feature flag")
    feature_name = feature_flag[3:]
    feature = None
    try:
        feature = await app.state.feature_registry.get_feature_by_name(feature_name)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    
    has_grant = await app.state.user_feature_service.has_grant(x_user_id, feature)
    return {
        "user_id": x_user_id,
        "feature": feature.name,
        "has_grant": has_grant
    }
    
